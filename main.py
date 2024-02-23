#! /usr/bin/env python3
import os
import stat
import datetime
import pymongo
import gridfs
from gridfs import GridFS
# output screen width 79 height 20



class BadEpub(Exception):
    """File is invalid"""


def get_choice(prompt: str, max_choice: int) -> int:
    while True:
        which = input(prompt)
        try:
            which = int(which)
            if 0 < which <= max_choice:
                return which
            print("Invalid choice!")
        except ValueError:
            print("Invalid choice!")


def initialize_database(*, session, db):
    if "books" in db.list_collection_names(session=session):
        return
    db.create_collection("books", session=session)
    if "books" not in db.list_collection_names(session=session):
        raise RuntimeError("Failed to create books collection")
    return


def get_file_by_id(*, db, session, file_id, file_name):
    fs = gridfs.GridFS(db)
    output_file_name = "./books_download/" + file_name
    # if the output file already exists, remove it
    try:
        os.remove(output_file_name)
    except FileNotFoundError:
        pass

    with open(output_file_name, "wb") as output_file:
        output_file.write(fs.get(file_id, session=session).read())

    os.chmod(output_file_name, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)


def save_file_gridfs(*, db, session, file_name, file_path):
    """Read in file_name and write it back out again"""
    if file_path[-5:] != ".epub" and file_path[-5:] != ".epub":
        print("-"*79)
        raise BadEpub(f"File {file_path} is not an epub or pdf file.")

    file_name_filter = {"filename": file_name}
    fs: GridFS = gridfs.GridFS(db)
    if fs.exists(file_name_filter, session=session):
        the_id = fs.find_one(file_name_filter, no_cursor_timeout=True, session=session)._id
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
        try:
            i["file_id"] = save_file_gridfs(
                db=db,
                session=session,
                file_name=i["file_name"],
                file_path=i["file_path"],
            )
        except BadEpub as error_message:
            raise BadEpub(error_message)
        db.books.insert_one(i, session=session)


def get_book_by_title(*, session, db, title):
    print(db.books.find_one({"title": title}, session=session))


def get_book_by_id(*, session, db, book_id):
    print(db.books.find_one({"_id": book_id}, session=session))


def add_book_menu(*, session, db):
    print("-" * 79)
    print("Add a book")
    print("-" * 79)
    book = {
        "title": input("Enter the title of the book: "),
        "language": input("Enter the language of the book: "),
        "published_date": input("Enter the published date of the book: "),
        "copy_right": input("Enter the copy-right of the book: "),
        "file_name": input("Enter the file name of the book: "),
        "file_path": input("Enter the file path of the book: ")
    }
    authors = []
    while True:
        author = {
            "name": input("Enter the name of the author: "),
            "pseudonym": input("Enter the pseudonym of the author: ")
        }
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
    except BadEpub as error_message:
        print(error_message)
        return


def list_book_pagination(*,
                         session: pymongo.mongo_client.client_session,
                         db: pymongo.mongo_client.database.Database,
                         page: int = 1,
                         page_size: int = 10
                         ):

    books = db.books.aggregate([
        {
            "$facet": {
                "metadata": [
                    {"$count": "total_count"},
                    {"$addFields": {"page": page}},
                ],
                "data": [
                    {"$skip": (page - 1) * page_size},
                    {"$limit": page_size},
                ],
            }
        }
    ], session=session)
    books_data = list(books)
    return books_data[0]["metadata"][0], books_data[0]["data"]


def list_books_menu(*, session, db):
    print("-" * 79)
    print("List all books")
    page = 1
    page_size = 10
    while True:
        print("-" * 79)
        metadata, data = list_book_pagination(session=session, db=db, page=page, page_size=page_size)
        total_page = metadata["total_count"] // page_size
        if metadata["total_count"] % page_size != 0:
            total_page += 1

        for i in range(len(data)):
            print(f"{i+1}. {data[i]['title']}")
        print("-" * 79)
        print(f"Page {page} of {total_page}")
        print("-" * 79)

        i = len(data)
        print(f"{i+1}. Next Page")

        if page > 1:
            print(f"{i+2}. Previous Page")
            print(f"{i+3}. Back to Main Menu")
            print("-" * 79)
            choice = get_choice("Enter your choice: ", i+3)
            if choice == i+1:
                page += 1
            elif choice == i+2:
                page -= 1
            elif choice == i+3:
                break
        else:
            print(f"{i+2}. Back to Main Menu")
            print("-" * 79)
            choice = get_choice("Enter your choice: ", i+2)
            if choice == i+1:
                page += 1
            elif choice == i+2:
                break


def menu():
    print("-" * 79)
    print("Main Menu")
    print("-" * 79)
    print("1. Add a book")
    print("2. List all books")
    print("3. Search for a book")
    print("4. Exit")
    print("-" * 79)
    for i in (range(11)):
        print()
    choice = get_choice("Enter your choice: ", 4)
    return choice


def main():
    with pymongo.MongoClient(URI) as client, client.start_session(causal_consistency=True) as session:
        db = client.get_database("books")
        db.drop_collection("books")
        try:
            initialize_database(session=session, db=db)
        except RuntimeError as error_message:
            print("Failed to initialize database: ", error_message)
            return EXIT_FAILURE

        try:
            add_books(session=session, db=db, books=BOOKS_DATA)
        except BadEpub as error_message:
            print(error_message)
            return EXIT_FAILURE

        try:
            while True:
                choice = menu()
                if choice == 1:
                    add_book_menu(session=session, db=db)
                elif choice == 2:
                    list_books_menu(session=session, db=db)
                elif choice == 3:
                    print("Search for a book")
                elif choice == 4:
                    print("Goodbye!")
                    break
        except KeyboardInterrupt:
            print("")
            print("Goodbye!")
    return EXIT_SUCCESS


books_schema = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["Title", "Author", "Language", "ISBN", "Published_date", "Genres", "Sub_genres", "Copy_right", "Main_characters", "File_name", "File_id"],
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
                        "pseudonym": {
                            "bsonType": "string",
                            "description": "Pseudonym of the author",
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
            "ISBN": {
                "bsonType": "string",
                "description": "ISBN of the book",
            },
            "main_characters": {
                "bsonType": "array",
                "items": {"bsonType": "string", "description": "Main characters of the book"},
                "description": "Main characters of the book",
            },
            "file_name": {
                "bsonType": "string",
                "description": "File name of the book",
            },
            "file_id": {
                "bsonType": "objectId",
                "description": "File id of the book",
            }
        },
    }
}

BOOKS_DATA = [
    {
        "title": "Frankenstein; Or, The Modern Prometheus",
        "author": [
            {"name": "Mary Wollstonecraft Shelley", "pseudonym": "Mary Shelley"}
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
        "ISBN": "978-1-59308-510-1",
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
        "ISBN": "978-1-59308-510-1",

    },
    {
        "title": "book_test2",
        "author": [{"name": "test2 test2"}],
        "language": "test2",
        "published_date": datetime.datetime(2001, 7, 1),
        "genres": ["Adventure"],
        "sub_genres": ["Whales"],
        "main_characters": ["Captain Ahab", "Ishmael", "Queequeg", "Moby Dick"],
        "copy_right": "Public domain in the USA.",
        "file_name": "Test2.epub",
        "file_path": "./books/Moby-Dick.epub",
        "ISBN": "978-1-59308-510-1",
    },
{
        "title": "book_test3",
        "author": [{"name": "test3 test3"}],
        "language": "test3",
        "published_date": datetime.datetime(2003, 9, 15),
        "genres": ["Mystery"],
        "sub_genres": ["Detective"],
        "main_characters": ["Sherlock Holmes", "Dr. Watson"],
        "copy_right": "Public domain in the USA.",
        "file_name": "Test3.epub",
        "file_path": "./books/Moby-Dick.epub",
        "ISBN": "978-1-59308-510-1",
    },
    {
        "title": "book_test4",
        "author": [{"name": "test4 test4"}],
        "language": "test4",
        "published_date": datetime.datetime(2005, 11, 23),
        "genres": ["Sci-Fi"],
        "sub_genres": ["Space Opera"],
        "main_characters": ["Captain Kirk", "Spock", "Dr. McCoy"],
        "copy_right": "Public domain in the USA.",
        "file_name": "Test4.epub",
        "file_path": "./books/Moby-Dick.epub",
        "ISBN": "978-1-59308-510-1",
    },
    {
        "title": "book_test5",
        "author": [{"name": "test5 test5"}],
        "language": "test5",
        "published_date": datetime.datetime(2007, 2, 14),
        "genres": ["Romance"],
        "sub_genres": ["Historical Romance"],
        "main_characters": ["Elizabeth Bennet", "Mr. Darcy"],
        "copy_right": "Public domain in the USA.",
        "file_name": "Test5.epub",
        "file_path": "./books/Moby-Dick.epub",
        "ISBN": "978-1-59308-510-1",
    },
    {
        "title": "book_test6",
        "author": [{"name": "test6 test6"}],
        "language": "test6",
        "published_date": datetime.datetime(2009, 5, 20),
        "genres": ["Fantasy"],
        "sub_genres": ["Epic Fantasy"],
        "main_characters": ["Frodo Baggins", "Gandalf", "Aragorn"],
        "copy_right": "Public domain in the USA.",
        "file_name": "Test6.epub",
        "file_path": "./books/Moby-Dick.epub",
        "ISBN": "978-1-59308-510-1",
    },
{
        "title": "book_test7",
        "author": [{"name": "test7 test7"}],
        "language": "test7",
        "published_date": datetime.datetime(2011, 8, 5),
        "genres": ["Horror"],
        "sub_genres": ["Gothic Horror"],
        "main_characters": ["Dracula", "Van Helsing"],
        "copy_right": "Public domain in the USA.",
        "file_name": "Test7.epub",
        "file_path": "./books/Moby-Dick.epub",
        "ISBN": "978-1-59308-510-1",

    },
    {
        "title": "book_test8",
        "author": [{"name": "test8 test8"}],
        "language": "test8",
        "published_date": datetime.datetime(2013, 10, 31),
        "genres": ["Thriller"],
        "sub_genres": ["Psychological Thriller"],
        "main_characters": ["Clarice Starling", "Hannibal Lecter"],
        "copy_right": "Public domain in the USA.",
        "file_name": "Test8.epub",
        "file_path": "./books/Moby-Dick.epub",
    },
    {
        "title": "book_test9",
        "author": [{"name": "test9 test9"}],
        "language": "test9",
        "published_date": datetime.datetime(2015, 12, 25),
        "genres": ["Non-fiction"],
        "sub_genres": ["Biography"],
        "main_characters": ["Albert Einstein"],
        "copy_right": "Public domain in the USA.",
        "file_name": "Test9.epub",
        "file_path": "./books/Moby-Dick.epub",
    },
    {
        "title": "book_test10",
        "author": [{"name": "test10 test10"}],
        "language": "test10",
        "published_date": datetime.datetime(2017, 3, 17),
        "genres": ["Comedy"],
        "sub_genres": ["Satire"],
        "main_characters": ["Bertie Wooster", "Jeeves"],
        "copy_right": "Public domain in the USA.",
        "file_name": "Test10.epub",
        "file_path": "./books/Moby-Dick.epub",
    },
    {
        "title": "book_test11",
        "author": [{"name": "test11 test11"}],
        "language": "test11",
        "published_date": datetime.datetime(2019, 6, 21),
        "genres": ["Children's Literature"],
        "sub_genres": ["Fairy Tales"],
        "main_characters": ["Cinderella", "Snow White"],
        "copy_right": "Public domain in the USA.",
        "file_name": "Test11.epub",
        "file_path": "./books/Moby-Dick.epub",
    },
    {
        "title": "book_test12",
        "author": [{"name": "test12 test12"}],
        "language": "test12",
        "published_date": datetime.datetime(2021, 9, 30),
        "genres": ["Poetry"],
        "sub_genres": ["Epic Poetry"],
        "main_characters": ["Odysseus"],
        "copy_right": "Public domain in the USA.",
        "file_name": "Test12.epub",
        "file_path": "./books/Moby-Dick.epub",
    },
    {
        "title": "book_test13",
        "author": [{"name": "test13 test13"}],
        "language": "test13",
        "published_date": datetime.datetime(2022, 1, 1),
        "genres": ["Historical Fiction"],
        "sub_genres": ["Military History"],
        "main_characters": ["Napoleon Bonaparte", "Duke of Wellington"],
        "copy_right": "Public domain in the USA.",
        "file_name": "Test13.epub",
        "file_path": "./books/Moby-Dick.epub",
    },
    {
        "title": "book_test14",
        "author": [{"name": "test14 test14"}],
        "language": "test14",
        "published_date": datetime.datetime(2022, 4, 15),
        "genres": ["Science"],
        "sub_genres": ["Astronomy"],
        "main_characters": ["Neil Armstrong", "Buzz Aldrin"],
        "copy_right": "Public domain in the USA.",
        "file_name": "Test14.epub",
        "file_path": "./books/Moby-Dick.epub",
    },
    {
        "title": "book_test15",
        "author": [{"name": "test15 test15"}],
        "language": "test15",
        "published_date": datetime.datetime(2022, 7, 4),
        "genres": ["Self-help"],
        "sub_genres": ["Motivational"],
        "main_characters": ["Tony Robbins"],
        "copy_right": "Public domain in the USA.",
        "file_name": "Test15.epub",
        "file_path": "./books/Moby-Dick.epub",
    },
    {
        "title": "book_test16",
        "author": [{"name": "test16 test16"}],
        "language": "test16",
        "published_date": datetime.datetime(2022, 10, 31),
        "genres": ["True Crime"],
        "sub_genres": ["Investigative Journalism"],
        "main_characters": ["Ted Bundy"],
        "copy_right": "Public domain in the USA.",
        "file_name": "Test16.epub",
        "file_path": "./books/Moby-Dick.epub",
    }
]


EXIT_SUCCESS = 0
EXIT_FAILURE = 1
URI = "mongodb://localhost:27017/"
if __name__ == "__main__":
    SystemExit(main())
