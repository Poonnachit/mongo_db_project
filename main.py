#! /usr/bin/env python3
import os
import stat
import datetime
import pymongo
import gridfs


class BadEpub(Exception):
    """File is invalid"""


def get_choice(prompt: str, maxchoice: int) -> int:
    while True:
        which = input(prompt)
        try:
            which = int(which)
            if 0 < which <= maxchoice:
                return which
            print("Invalid choice!")
        except ValueError:
            print("Invalid choice!")


def initialize_database(*, client, session, db):
    if "books" in db.list_collection_names(session=session):
        return
    db.create_collection("books", session=session)
    if "books" not in db.list_collection_names(session=session):
        raise RuntimeError("Failed to create books collection")
    return


def get_file_by_id(*, db, session, file_id, file_name):
    fs = gridfs.GridFS(db)
    outfname = "./books_download/" + file_name
    # if the output file already exists, remove it
    try:
        os.remove(outfname)
    except FileNotFoundError:
        pass

    with open(outfname, "wb") as outfile:
        outfile.write(fs.get(file_id, session=session).read())

    os.chmod(outfname, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)

    tfile = fs.get_last_version(filename=file_name, session=session)
    _id, filename, filelength, upload_date, content_type = (
        tfile._id,
        tfile.name,
        tfile.length,
        tfile.upload_date,
        tfile.content_type,
    )
    tfile.close()


def save_file_gridfs(*, db, session, file_name, file_path):
    """Read in file_name and write it back out again"""
    if not file_name.endswith(".epub"):
        raise BadEpub(f'specified file "{file_name}" is not an epub file.')

    filter = {"filename": file_name}
    fs = gridfs.GridFS(db)
    if fs.exists(filter, session=session):
        the_id = fs.find_one(filter, no_cursor_timeout=True, session=session)._id
        fs.delete(the_id, session=session)
        del the_id
    if os.path.isdir(file_path):
        raise BadEpub(f'specified file "{file_path}" is a directory, not a file.')
    try:
        with open(file_path, "rb") as infile:
            the_id = fs.put(
                infile.read(),
                filename=file_name,
                content_type="EPUB Document",
                session=session,
            )
            if the_id is None:
                raise BadEpub(f'failed to save file "{file_path}" to GridFS.')
    except FileNotFoundError:
        raise BadEpub(f'specified file "{file_path}" does not exist.')

    return the_id


def add_books(*, session, db, books):
    for i in books:
        i["file_id"] = save_file_gridfs(
            db=db,
            session=session,
            file_name=i["file_name"],
            file_path=i["file_path"],
        )
        db.books.insert_one(i, session=session)
        # get_file_by_id(
        #     db=db, session=session, file_id=i["file_id"], file_name=i["file_name"]
        # )


def add_book_menu(*, session, db):
    print("-" * 80)
    print("Add a book")
    print("-" * 80)
    book = {}
    book["title"] = input("Enter the title of the book: ")
    book["language"] = input("Enter the language of the book: ")
    book["published_date"] = input("Enter the published date of the book: ")
    book["copy_right"] = input("Enter the copy-right of the book: ")
    book["file_name"] = input("Enter the file name of the book: ")
    book["file_path"] = input("Enter the file path of the book: ")
    authors = []
    while True:
        author = {}
        author["name"] = input("Enter the name of the author: ")
        author["psuedonym"] = input("Enter the psuedonym of the author: ")
        authors.append(author)
        while True:
            again = input("Add another Author? (y/n): ")
            if again not in ["y", "Y", "n", "N"]:
                print("Invalid input")
                continue
            break
        if again == "n":
            break
    book["author"] = authors
    genres = []
    while True:
        genre = input("Enter the genre of the book: ")
        genres.append(genre)
        while True:
            again = input("Add another genre? (y/n): ")
            if again not in ["y", "Y", "n", "N"]:
                print("Invalid input")
                continue
            break
        if again == "n":
            break

    book["genres"] = genres
    sub_genres = []
    while True:
        sub_genre = input("Enter the sub-genre of the book: ")
        sub_genres.append(sub_genre)
        while True:
            again = input("Add another sub-genre? (y/n): ")
            if again not in ["y", "Y", "n", "N"]:
                print("Invalid input")
                continue
            break
        if again == "n":
            break
    book["sub_genres"] = sub_genres

    try:
        add_books(session=session, db=db, books=[book])
    except BadEpub as excpn:
        print(excpn)
        return


def menu():
    print("1. Add a book")
    print("2. List all books")
    print("3. Search for a book")
    print("4. Remove a book")
    print("5. Exit")
    choice = get_choice("Enter your choice: ", 5)
    return choice


def main():
    return_code = EXIT_SUCCESS
    with pymongo.MongoClient(
        "mongodb://localhost:27017/"
    ) as client, client.start_session(causal_consistency=True) as session:
        db = client.get_database("books")
        db.drop_collection("books")
        try:
            initialize_database(client=client, session=session, db=db)
        except RuntimeError as excpn:
            print("Failed to initialize database: ", excpn)
            return_code = EXIT_FAILURE

        try:
            add_books(session=session, db=db, books=BOOKS_DATA)
        except BadEpub as excpn:
            print(excpn)
            return_code = EXIT_FAILURE

        try:
            while True:
                choice = menu()
                if choice == 1:
                    add_book_menu(session=session, db=db)
                elif choice == 2:
                    print("List all books")
                elif choice == 3:
                    print("Search for a book")
                elif choice == 4:
                    print("Remove a book")
                elif choice == 5:
                    print("Exit")
                    break
        except KeyboardInterrupt:
            print("")
            print("Goodbye!")

    return return_code


books_schema = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["Title", "Author", "Language"],
        "properties": {
            "title": {
                "bsonType": "string",
                "minimum": 1,
                "description": "Title of the book",
            },
            "author": {
                "bsonType": "array",
                "items": {
                    "bsonType": "object",
                    "required": ["name"],
                    "properties": {
                        "name": {
                            "bsonType": "string",
                            "minimum": 1,
                            "description": "Name of the author",
                        },
                        "psuedonym": {
                            "bsonType": "string",
                            "description": "Psuedonym of the author",
                        },
                    },
                },
                "description": "Authors of the book",
            },
            "language": {"bsonType": "string", "description": "Language of the book"},
            "published_date": {
                "bsonType": "date",
                "description": "Published date of the book",
            },
            "genres": {
                "bsonType": "array",
                "items": {"bsonType": "string", "description": "Genre of the book"},
                "description": "Genres of the book",
            },
            "sub_genres": {
                "bsonType": "array",
                "items": {"bsonType": "string", "description": "Sub-genre of the book"},
                "description": "Sub-genres of the book",
            },
            "copy_right": {
                "bsonType": "string",
                "description": "Copy-right of the book",
            },
        },
    }
}

BOOKS_DATA = [
    {
        "title": "Frankenstein; Or, The Modern Prometheus",
        "author": [
            {"name": "Mary Wollstonecraft Shelley", "psuedonym": "Mary Shelley"}
        ],
        "language": "English",
        "published_date": datetime.datetime(1993, 10, 1),
        "genres": ["Horror", "Gothic", "Science Fiction"],
        "sub_genres": ["Gothic Horror", "Science Fiction Horror"],
        "main_characters": [
            "Victor Frankenstein",
            "The Monster",
            "Elizabeth Lavenza",
            "Henry Clerval",
        ],
        "copy_right": "Public domain in the USA.",
        "file_name": "Frankenstein.epub",
        "file_path": "./books/Frankenstein.epub",
    },
    {
        "title": "Moby Dick; Or, The Whale",
        "author": [{"name": "Herman Melville"}],
        "language": "English",
        "published_date": datetime.datetime(2001, 7, 1),
        "genres": ["Adventure"],
        "sub_genres": ["Whales"],
        "main_characters": ["Captain Ahab", "Ishmael", "Queequeg", "Moby Dick"],
        "copy_right": "Public domain in the USA.",
        "file_name": "Moby-Dick.epub",
        "file_path": "./books/Moby-Dick.epub",
    },
]

EXIT_SUCCESS = 0
EXIT_FAILURE = 1

if __name__ == "__main__":
    SystemExit(main())