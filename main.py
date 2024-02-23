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
    """
    Get a choice from the user

    Args:
        prompt: Text to display to the user
        max_choice: Maximum choice the user can make

    Returns: The choice the user made
    """
    while True:
        which = input(prompt)
        try:
            which = int(which)
            if 0 < which <= max_choice:
                return which
            print("Invalid choice!")
        except ValueError:
            print("Invalid choice!")


def download_file_by_id(
    *,
    session: pymongo.mongo_client.client_session,
    db: pymongo.mongo_client.database.Database,
    file_id: str,
    file_name: str,
) -> str:
    """
    Download a file from GridFS by its id to books_download directory

    Args:
        session: session to connect to the database
        db: use in which database
        file_id: file id to download
        file_name: what to name the file

    Returns: The path to the downloaded file
    """
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
    print(f"File {file_name} downloaded to books_download directory")
    return output_file_name


def save_file_gridfs(
    *,
    session: pymongo.mongo_client.client_session,
    db: pymongo.mongo_client.database.Database,
    file_name: str,
    file_path: str,
) -> str:
    """
    Save a file to GridFS

    Args:
        session: session to connect to the database
        db: use in which database
        file_name: name of the file
        file_path: path to the file

    Returns: The id of the file in GridFS
    """
    if file_path[-5:] != ".epub" and file_path[-5:] != ".pdf":
        print("-" * 79)
        raise BadEpub(f"File {file_path} is not an epub or pdf file.")

    file_name_filter = {"filename": file_name}
    fs: GridFS = gridfs.GridFS(db)
    if fs.exists(file_name_filter, session=session):
        the_id = fs.find_one(
            file_name_filter, no_cursor_timeout=True, session=session
        )._id
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


def delete_file_gridfs(
    *,
    session: pymongo.mongo_client.client_session,
    db: pymongo.mongo_client.database.Database,
    file_id: str,
) -> None:
    """
    Delete a file from GridFS

    Args:
        session: session to connect to the database
        db: use in which database
        file_id: id of the file to delete

    Returns: None
    """
    fs = gridfs.GridFS(db)
    fs.delete(file_id, session=session)
    return


def add_books(
    *,
    session: pymongo.mongo_client.client_session,
    db: pymongo.mongo_client.database.Database,
    books: list,
) -> None:
    """
    Add books to the database

    Args:
        session: session to connect to the database
        db: use in which database
        books: list of books to add to the database

    Returns: None
    """
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
        if i["file_path"][-5:] == ".epub":
            i["file_type"] = "EPUB"
        elif i["file_path"][-4:] == ".pdf":
            i["file_type"] = "PDF"

        db.books.insert_one(i, session=session)

    return


def add_book_menu(
    *,
    session: pymongo.mongo_client.client_session,
    db: pymongo.mongo_client.database.Database,
) -> None:
    """
    Add a book to the database Menu from user input

    Args:
        session: session to connect to the database
        db: use in which database

    Returns: None
    """
    print("-" * 79)
    print("Add a book")
    print("-" * 79)

    book = {}

    while True:
        book["title"] = input("Enter the title of the book (required): ")
        if book["title"] == "":
            print("Invalid input")
            continue
        break

    authors = []
    while True:
        author = {"name": input("Enter the name of the author (required): ")}
        if author["name"] == "":
            print("Invalid input")
            continue
        author["pseudonym"] = input("Enter the pseudonym of the author: ")
        if author["pseudonym"] == "":
            del author["pseudonym"]
        authors.append(author)
        while True:
            again = input("Add another Author? (y/n): ")
            if again not in ["y", "Y", "n", "N"]:
                print("Invalid input")
                continue
            break
        if again in ["n", "N"]:
            break
    book["author"] = authors

    while True:
        book["language"] = input("Enter the language of the book (required): ")
        if book["language"] == "":
            print("Invalid input")
            continue
        break

    genres = []
    while True:
        genre = input("Enter the genre of the book (required): ")
        if genre == "":
            print("Invalid input")
            continue
        genres.append(genre)
        while True:
            again = input("Add another genre? (y/n): ")
            if again not in ["y", "Y", "n", "N"]:
                print("Invalid input")
                continue
            break
        if again in ["n", "N"]:
            break
    book["genres"] = genres

    sub_genres = []
    while True:
        sub_genre = input("Enter the sub-genre of the book (required): ")
        if sub_genre == "":
            print("Invalid input")
            continue
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

    main_characters = []
    while True:
        main_character = input("Enter the main character of the book (required): ")
        if main_character == "":
            print("Invalid input")
            continue
        main_characters.append(main_character)
        while True:
            again = input("Add another main character? (y/n): ")
            if again not in ["y", "Y", "n", "N"]:
                print("Invalid input")
                continue
            break
        if again == "n":
            break
    book["main_characters"] = main_characters

    while True:
        published_date = input("Enter the published date of the book (required): ")
        if published_date == "":
            print("Invalid input (YYYY/MM/DD) Example. 1993/10/01")
            continue
        # if published_data not YYYY/MM/DD
        if len(published_date) != 10:
            print("Invalid input (YYYY/MM/DD) Example. 1993/10/01")
            continue
        if not published_date[0:4].isdigit():
            print("Invalid input (YYYY/MM/DD) Example. 1993/10/01")
            continue
        if not published_date[5:7].isdigit():
            print("Invalid input (YYYY/MM/DD) Example. 1993/10/01")
            continue
        if not published_date[8:10].isdigit():
            print("Invalid input (YYYY/MM/DD) Example. 1993/10/01")
            continue
        if published_date[4] != "/" or published_date[7] != "/":
            print("Invalid input (YYYY/MM/DD) Example. 1993/10/01")
            continue
        book["published_date"] = datetime.datetime.strptime(published_date, "%Y/%m/%d")
        break

    book["set_year"] = input("Enter the set year of the book: ")
    book["set_main_location"] = input("Enter the set country of the book: ")
    book["copy_right"] = input("Enter the copy-right of the book: ")
    while True:
        book["ISBN"] = input("Enter the ISBN of the book (required): ")
        if book["ISBN"] == "":
            print("Invalid input")
            continue
        break

    while True:
        book["file_name"] = input("Enter the file name of the book (required): ")
        if book["file_name"] == "":
            print("Invalid input")
            continue
        if book["file_name"][-5:] != ".epub" and book["file_name"][-4:] != ".pdf":
            print("Invalid file name (must end with .epub or .pdf)")
            continue
        break

    while True:
        book["file_path"] = input("Enter the file path of the book (required): ")
        if book["file_path"] == "":
            print("Invalid input")
            continue
        if book["file_path"][-5:] != ".epub" and book["file_path"][-4:] != ".pdf":
            print("Invalid file path (must end with .epub or .pdf)")
            continue
        elif not os.path.isfile(book["file_path"]):
            print("Invalid file path")
            continue
        break

    try:
        add_books(session=session, db=db, books=[book])
        print("Book added")
    except BadEpub as error_message:
        print(error_message)
        return


def get_book_data(
    *,
    session: pymongo.mongo_client.client_session,
    db: pymongo.mongo_client.database.Database,
    book_id: str,
) -> dict:
    """
    Get the data of a book from the database
    Args:
        session: session to connect to the database
        db: use in which database
        book_id: id of the book

    Returns: The data of the book
    """
    book = db.books.find_one({"_id": book_id}, session=session)

    return book


def edit_book_title(
    *,
    session: pymongo.mongo_client.client_session,
    db: pymongo.mongo_client.database.Database,
    book_id: str,
):
    """
    Edit the title of a book
    Args:
        session: session to connect to the database
        db: use in which database
        book_id: id of the book
    """
    print("-" * 79)
    print("Edit Title")
    print("-" * 79)
    book = get_book_data(session=session, db=db, book_id=book_id)
    print(f"Current Title: {book['title']}")
    while True:
        new_title = input("Enter the new title: ")
        if new_title == "":
            print("Invalid input")
            continue
        break

    db.books.update_one(
        {"_id": book_id},
        {"$set": {"title": new_title}},
    )
    print("Title updated")


def edit_book_author(
    *,
    session: pymongo.mongo_client.client_session,
    db: pymongo.mongo_client.database.Database,
    book_id: str,
):
    """
    Edit the author of a book
    Args:
        session: session to connect to the database
        db: use in which database
        book_id: id of the book
    """
    print("-" * 79)
    print("Edit Author")
    print("-" * 79)
    book = get_book_data(session=session, db=db, book_id=book_id)
    print(f"Current Author: ")
    for i in range(len(book["author"])):
        author = book["author"][i]
        if "pseudonym" in author:
            print(f"{author['name']} ({author['pseudonym']})")
        else:
            print(f"{author['name']}")
    print("1. Add Author")
    print("2. Remove Author")
    print("3. Back")
    choice = get_choice("Enter your choice: ", 3)
    if choice == 1:
        new_author = []
        while True:
            author = {"name": input("Enter the name of the author: ")}
            if author["name"] == "":
                print("Invalid input")
                continue
            author["pseudonym"] = input("Enter the pseudonym of the author: ")
            if author["pseudonym"] == "":
                del author["pseudonym"]
            new_author.append(author)
            while True:
                again = input("Add another Author? (y/n): ")
                if again not in ["y", "Y", "n", "N"]:
                    print("Invalid input")
                    continue
                break
            if again in ["n", "N"]:
                break

        db.books.update_one(
            {"_id": book_id},
            {"$push": {"author": {"$each": new_author}}},
        )
        print("Author added")
    elif choice == 2:
        print("-" * 79)
        print("Remove Author")
        print("-" * 79)
        for i in range(len(book["author"])):
            author = book["author"][i]
            if "pseudonym" in author:
                print(f"{i+1}. {author['name']} ({author['pseudonym']})")
            else:
                print(f"{i+1}. {author['name']}")
        print("which author do you want to remove?")
        choice = get_choice("Enter your choice: ", len(book["author"]))
        db.books.update_one(
            {"_id": book_id},
            {"$pull": {"author": book["author"][choice - 1]}},
        )

        print("Author removed")
    elif choice == 3:
        pass


def edit_book_language(
    *,
    session: pymongo.mongo_client.client_session,
    db: pymongo.mongo_client.database.Database,
    book_id: str,
):
    """
    Edit the language of a book
    Args:
        session: session to connect to the database
        db: use in which database
        book_id: id of the book
    """
    print("-" * 79)
    print("Edit Language")
    print("-" * 79)
    book = get_book_data(session=session, db=db, book_id=book_id)
    print(f"Current Language: {book['language']}")
    while True:
        new_language = input("Enter the new language: ")
        if new_language == "":
            print("Invalid input")
            continue
        break

    db.books.update_one(
        {"_id": book_id},
        {"$set": {"language": new_language}},
    )
    print("Language updated")


def edit_published_date(
    *,
    session: pymongo.mongo_client.client_session,
    db: pymongo.mongo_client.database.Database,
    book_id: str,
):
    """
    Edit the published date of a book
    Args:
        session: session to connect to the database
        db: use in which database
        book_id: id of the book
    """
    print("-" * 79)
    print("Edit Published Date")
    print("-" * 79)
    book = get_book_data(session=session, db=db, book_id=book_id)
    print(f"Current Published Date: {book['published_date']}")
    while True:
        published_date = input("Enter the published date of the book (required): ")
        if published_date == "":
            print("Invalid input (YYYY/MM/DD) Example. 1993/10/01")
            continue
        # if published_data not YYYY/MM/DD
        if len(published_date) != 10:
            print("Invalid input (YYYY/MM/DD) Example. 1993/10/01")
            continue
        if not published_date[0:4].isdigit():
            print("Invalid input (YYYY/MM/DD) Example. 1993/10/01")
            continue
        if not published_date[5:7].isdigit():
            print("Invalid input (YYYY/MM/DD) Example. 1993/10/01")
            continue
        if not published_date[8:10].isdigit():
            print("Invalid input (YYYY/MM/DD) Example. 1993/10/01")
            continue
        if published_date[4] != "/" or published_date[7] != "/":
            print("Invalid input (YYYY/MM/DD) Example. 1993/10/01")
            continue
        db.books.update_one(
            {"_id": book_id},
            {
                "$set": {
                    "published_date": datetime.datetime.strptime(
                        published_date, "%Y/%m/%d"
                    )
                }
            },
        )
        print("Published Date updated")
        break


def edit_genres(
    *,
    session: pymongo.mongo_client.client_session,
    db: pymongo.mongo_client.database.Database,
    book_id: str,
):
    """
    Edit the genres of a book
    Args:
        session: session to connect to the database
        db: use in which database
        book_id: id of the book
    """
    print("-" * 79)
    print("Edit Genres")
    print("-" * 79)
    book = get_book_data(session=session, db=db, book_id=book_id)
    print(f"Current Genres: ")
    for i in range(len(book["genres"])):
        print(f"{i+1}. {book['genres'][i]}")
    print("1. Add Genre")
    print("2. Remove Genre")
    print("3. Back")
    choice = get_choice("Enter your choice: ", 3)
    if choice == 1:
        new_genres = []
        while True:
            genre = input("Enter the genre of the book: ")
            if genre == "":
                print("Invalid input")
                continue
            new_genres.append(genre)
            while True:
                again = input("Add another genre? (y/n): ")
                if again not in ["y", "Y", "n", "N"]:
                    print("Invalid input")
                    continue
                break
            if again in ["n", "N"]:
                break

        db.books.update_one(
            {"_id": book_id},
            {"$push": {"genres": {"$each": new_genres}}},
        )
        print("Genre added")
    elif choice == 2:
        print("-" * 79)
        print("Remove Genre")
        print("-" * 79)
        for i in range(len(book["genres"])):
            print(f"{i+1}. {book['genres'][i]}")
        print("which genre do you want to remove?")
        choice = get_choice("Enter your choice: ", len(book["genres"]))
        db.books.update_one(
            {"_id": book_id},
            {"$pull": {"genres": book["genres"][choice - 1]}},
        )

        print("Genre removed")
    elif choice == 3:
        pass


def edit_sub_genres(
    *,
    session: pymongo.mongo_client.client_session,
    db: pymongo.mongo_client.database.Database,
    book_id: str,
):
    """
    Edit the sub-genres of a book
    Args:
        session: session to connect to the database
        db: use in which database
        book_id: id of the book
    """
    print("-" * 79)
    print("Edit Sub-genres")
    print("-" * 79)
    book = get_book_data(session=session, db=db, book_id=book_id)
    print(f"Current Sub-genres: ")
    for i in range(len(book["sub_genres"])):
        print(f"{i+1}. {book['sub_genres'][i]}")
    print("1. Add Sub-genre")
    print("2. Remove Sub-genre")
    print("3. Back")
    choice = get_choice("Enter your choice: ", 3)
    if choice == 1:
        new_sub_genres = []
        while True:
            sub_genre = input("Enter the sub-genre of the book: ")
            if sub_genre == "":
                print("Invalid input")
                continue
            new_sub_genres.append(sub_genre)
            while True:
                again = input("Add another sub-genre? (y/n): ")
                if again not in ["y", "Y", "n", "N"]:
                    print("Invalid input")
                    continue
                break
            if again in ["n", "N"]:
                break

        db.books.update_one(
            {"_id": book_id},
            {"$push": {"sub_genres": {"$each": new_sub_genres}}},
        )
        print("Sub-genre added")
    elif choice == 2:
        print("-" * 79)
        print("Remove Sub-genre")
        print("-" * 79)
        for i in range(len(book["sub_genres"])):
            print(f"{i+1}. {book['sub_genres'][i]}")
        print("which sub-genre do you want to remove?")
        choice = get_choice("Enter your choice: ", len(book["sub_genres"]))
        db.books.update_one(
            {"_id": book_id},
            {"$pull": {"sub_genres": book["sub_genres"][choice - 1]}},
        )

        print("Sub-genre removed")
    elif choice == 3:
        pass


def edit_main_characters(
    *,
    session: pymongo.mongo_client.client_session,
    db: pymongo.mongo_client.database.Database,
    book_id: str,
):
    """
    Edit the main characters of a book
    Args:
        session: session to connect to the database
        db: use in which database
        book_id: id of the book
    """
    print("-" * 79)
    print("Edit Main Characters")
    print("-" * 79)
    book = get_book_data(session=session, db=db, book_id=book_id)
    print(f"Current Main Characters: ")
    for i in range(len(book["main_characters"])):
        print(f"{i+1}. {book['main_characters'][i]}")
    print("1. Add Main Character")
    print("2. Remove Main Character")
    print("3. Back")
    choice = get_choice("Enter your choice: ", 3)
    if choice == 1:
        new_main_characters = []
        while True:
            main_character = input("Enter the main character of the book: ")
            if main_character == "":
                print("Invalid input")
                continue
            new_main_characters.append(main_character)
            while True:
                again = input("Add another main character? (y/n): ")
                if again not in ["y", "Y", "n", "N"]:
                    print("Invalid input")
                    continue
                break
            if again in ["n", "N"]:
                break

        db.books.update_one(
            {"_id": book_id},
            {"$push": {"main_characters": {"$each": new_main_characters}}},
        )
        print("Main character added")
    elif choice == 2:
        print("-" * 79)
        print("Remove Main Character")
        print("-" * 79)
        for i in range(len(book["main_characters"])):
            print(f"{i+1}. {book['main_characters'][i]}")
        print("which main character do you want to remove?")
        choice = get_choice("Enter your choice: ", len(book["main_characters"]))
        db.books.update_one(
            {"_id": book_id},
            {"$pull": {"main_characters": book["main_characters"][choice - 1]}},
        )

        print("Main character removed")
    elif choice == 3:
        pass


def edit_isbn(
    *,
    session: pymongo.mongo_client.client_session,
    db: pymongo.mongo_client.database.Database,
    book_id: str,
):
    """
    Edit the ISBN of a book
    Args:
        session: session to connect to the database
        db: use in which database
        book_id: id of the book
    """
    print("-" * 79)
    print("Edit ISBN")
    print("-" * 79)
    book = get_book_data(session=session, db=db, book_id=book_id)
    print(f"Current ISBN: {book['ISBN']}")
    while True:
        new_isbn = input("Enter the new ISBN: ")
        if new_isbn == "":
            print("Invalid input")
            continue
        break

    db.books.update_one(
        {"_id": book_id},
        {"$set": {"ISBN": new_isbn}},
    )
    print("ISBN updated")


def edit_set_year(
    *,
    session: pymongo.mongo_client.client_session,
    db: pymongo.mongo_client.database.Database,
    book_id: str,
):
    """
    Edit the set year of a book
    Args:
        session: session to connect to the database
        db: use in which database
        book_id: id of the book
    """
    print("-" * 79)
    print("Edit Set Year")
    print("-" * 79)
    book = get_book_data(session=session, db=db, book_id=book_id)
    print(f"Current Set Year: {book['set_year']}")
    while True:
        new_set_year = input("Enter the new set year: ")
        if new_set_year == "":
            print("Invalid input")
            continue
        break

    db.books.update_one(
        {"_id": book_id},
        {"$set": {"set_year": new_set_year}},
    )
    print("Set Year updated")


def edit_set_main_location(
    *,
    session: pymongo.mongo_client.client_session,
    db: pymongo.mongo_client.database.Database,
    book_id: str,
):
    """
    Edit the set main location of a book
    Args:
        session: session to connect to the database
        db: use in which database
        book_id: id of the book
    """
    print("-" * 79)
    print("Edit Set Main Location")
    print("-" * 79)
    book = get_book_data(session=session, db=db, book_id=book_id)
    print(f"Current Set Main Location: {book['set_main_location']}")
    while True:
        new_set_main_location = input("Enter the new set main location: ")
        if new_set_main_location == "":
            print("Invalid input")
            continue
        break

    db.books.update_one(
        {"_id": book_id},
        {"$set": {"set_main_location": new_set_main_location}},
    )
    print("Set Main Location updated")


def edit_copy_right(
    *,
    session: pymongo.mongo_client.client_session,
    db: pymongo.mongo_client.database.Database,
    book_id: str,
):
    """
    Edit the copy right of a book
    Args:
        session: session to connect to the database
        db: use in which database
        book_id: id of the book
    """
    print("-" * 79)
    print("Edit Copy Right")
    print("-" * 79)
    book = get_book_data(session=session, db=db, book_id=book_id)
    print(f"Current Copy Right: {book['copy_right']}")
    while True:
        new_copy_right = input("Enter the new copy right: ")
        if new_copy_right == "":
            print("Invalid input")
            continue
        break

    db.books.update_one(
        {"_id": book_id},
        {"$set": {"copy_right": new_copy_right}},
    )
    print("Copy Right updated")


def edit_book_metadata(
    *,
    session: pymongo.mongo_client.client_session,
    db: pymongo.mongo_client.database.Database,
    book_id: str,
):
    """
    Edit the metadata of a book
    Args:
        session: session to connect to the database
        db: use in which database
        book_id: id of the book
    """
    print("-" * 79)
    print("Edit Metadata")
    print("-" * 79)
    book = get_book_data(session=session, db=db, book_id=book_id)
    print(f" 1. Title              | {book['title']}")
    print(f" 2. Author             |", end=" ")
    for i in range(len(book["author"])):
        author = book["author"][i]
        if i == 0:
            if "pseudonym" in author:
                print(f"{author['name']} ({author['pseudonym']})", end=" ")
            else:
                print(f"{author['name']}", end=" ")
        else:
            if "pseudonym" in author:
                print(
                    f"                       | {author['name']} ({author['pseudonym']})",
                    end=" ",
                )
            else:
                print(f"                       | {author['name']}", end=" ")
        print()
    print(f" 3. Language           | {book['language']}")
    print(f" 4. Published Date     | {book['published_date']}")
    print(f" 5. Genres             |", end=" ")
    for i in range(len(book["genres"])):
        if i == 0:
            print(book["genres"][i], end=" ")
        else:
            print(f"                       | {book['genres'][i]}", end=" ")
        print()
    print(f" 6. Sub-genres         |", end=" ")
    for i in range(len(book["sub_genres"])):
        if i == 0:
            print(book["sub_genres"][i], end=" ")
        else:
            print(f"                       | {book['sub_genres'][i]}", end=" ")
        print()
    print(f" 7. Main Characters    |", end=" ")
    for i in range(len(book["main_characters"])):
        if i == 0:
            print(book["main_characters"][i], end=" ")
        else:
            print(f"                       | {book['main_characters'][i]}", end=" ")
        print()

    print(f" 8. ISBN               | {book['ISBN']}")
    order_choice = 8
    if "set_year" in book:
        order_choice += 1
        print(f"{order_choice:2d}. Set Year           | {book['set_year']}")
    if "set_main_location" in book:
        order_choice += 1
        print(f"{order_choice:2d}. Set Main Location  | {book['set_main_location']}")
    if "copy_right" in book:
        order_choice += 1
        print(f"{order_choice:2d}. Copy-right         | {book['copy_right']}")
    order_choice += 1
    print(f"{order_choice:2d}. Back")
    print("-" * 79)
    choice = get_choice("Enter your choice: ", 12)
    if choice == 1:
        edit_book_title(session=session, db=db, book_id=book_id)
    elif choice == 2:
        edit_book_author(session=session, db=db, book_id=book_id)
    elif choice == 3:
        edit_book_language(session=session, db=db, book_id=book_id)
    elif choice == 4:
        edit_published_date(session=session, db=db, book_id=book_id)
    elif choice == 5:
        edit_genres(session=session, db=db, book_id=book_id)
    elif choice == 6:
        edit_sub_genres(session=session, db=db, book_id=book_id)
    elif choice == 7:
        edit_main_characters(session=session, db=db, book_id=book_id)
    elif choice == 8:
        edit_isbn(session=session, db=db, book_id=book_id)
    elif choice == 9:
        if "set_year" in book:
            edit_set_year(session=session, db=db, book_id=book_id)
        elif "set_main_location" in book:
            edit_set_main_location(session=session, db=db, book_id=book_id)
        elif "copy_right" in book:
            edit_copy_right(session=session, db=db, book_id=book_id)
    elif choice == 10:
        if "set_main_location" in book:
            edit_set_main_location(session=session, db=db, book_id=book_id)
        elif "copy_right" in book:
            edit_copy_right(session=session, db=db, book_id=book_id)
    elif choice == 11:
        edit_copy_right(session=session, db=db, book_id=book_id)
    elif choice == 12:
        pass


def change_book_file(
    *,
    session: pymongo.mongo_client.client_session,
    db: pymongo.mongo_client.database.Database,
    book_id: str,
):
    """
    Change the file of a book
    Args:
        session: session to connect to the database
        db: use in which database
        book_id: id of the book
    """
    print("-" * 79)
    print("Change Book File")
    print("-" * 79)
    book = get_book_data(session=session, db=db, book_id=book_id)
    print(f"Current File Name: {book['file_name']}")
    while True:
        new_file_name = input("Enter the new file name: ")
        if new_file_name == "":
            print("Invalid input")
            continue
        if new_file_name[-5:] != ".epub" and new_file_name[-4:] != ".pdf":
            print("Invalid file name (must end with .epub or .pdf)")
            continue
        break

    while True:
        new_file_path = input("Enter the new file path: ")
        if new_file_path == "":
            print("Invalid input")
            continue
        if new_file_path[-5:] != ".epub" and new_file_path[-4:] != ".pdf":
            print("Invalid file path (must end with .epub or .pdf)")
            continue
        elif not os.path.isfile(new_file_path):
            print("Invalid file path")
            continue
        break

    try:
        file_id = save_file_gridfs(
            db=db,
            session=session,
            file_name=new_file_name,
            file_path=new_file_path,
        )
    except BadEpub as error_message:
        print(error_message)
        return

    if new_file_path[-5:] == ".epub":
        new_file_type = "EPUB"
    elif new_file_path[-4:] == ".pdf":
        new_file_type = "PDF"

    db.books.update_one(
        {"_id": book_id},
        {
            "$set": {
                "file_id": file_id,
                "file_name": new_file_name,
                "file_path": new_file_path,
                "file_type": new_file_type,
            }
        },
    )
    print("File updated")


def delete_book(
    *,
    session: pymongo.mongo_client.client_session,
    db: pymongo.mongo_client.database.Database,
    book_id: str,
):
    print("-" * 79)
    print("Delete Book")
    print("-" * 79)
    book = get_book_data(session=session, db=db, book_id=book_id)
    print(f"Title: {book['title']}")
    print(f"Author:")
    for i in range(len(book["author"])):
        author = book["author"][i]
        if "pseudonym" in author:
            print(f"   - {author['name']} ({author['pseudonym']})")
        else:
            print(f"   - {author['name']}")
    print("Do you want to delete this book?")
    print("1. Yes")
    print("2. No")
    choice = get_choice("Enter your choice: ", 2)
    if choice == 1:
        delete_file_gridfs(db=db, session=session, file_id=book["file_id"])
        db.books.delete_one({"_id": book_id})
        print("Book deleted")
    elif choice == 2:
        pass


def book_data_menu(
    *,
    session: pymongo.mongo_client.client_session,
    db: pymongo.mongo_client.database.Database,
    book_id: str,
):
    """
    Print the data of a book and ask the user what to do next
    Args:
        session: session to connect to the database
        db: use in which database
        book_id: id of the book
    """
    print("-" * 79)
    print("Book Data")
    print("-" * 79)
    book = get_book_data(session=session, db=db, book_id=book_id)
    for key, value in book.items():
        if key in ["_id", "file_id", "file_path"]:
            continue
        elif key == "author":
            print(f"{key}:")
            for author in value:
                if "pseudonym" in author:
                    print(f"   - {author['name']} ({author['pseudonym']})")
                else:
                    print(f"   - {author['name']}")
            continue
        elif key in ["genres", "sub_genres", "main_characters"]:
            print(f"{key}:")
            for v in value:
                print(f"   - {v}")
            continue
        else:
            print(f"{key}: {value}")
    print("-" * 79)
    print("1. Edit Metadata")
    print("2. Change Book File")
    print("3. Download Book")
    print("4. Delete Book")
    print("5. Back")
    print("-" * 79)
    choice = get_choice("Enter your choice: ", 5)
    match choice:
        case 1:
            edit_book_metadata(session=session, db=db, book_id=book_id)
        case 2:
            change_book_file(session=session, db=db, book_id=book_id)
        case 3:
            download_file_by_id(
                db=db,
                session=session,
                file_id=book["file_id"],
                file_name=book["file_name"],
            )
        case 4:
            delete_book(session=session, db=db, book_id=book_id)

        case 5:
            pass


def list_book_pagination(
    *,
    session: pymongo.mongo_client.client_session,
    db: pymongo.mongo_client.database.Database,
    page: int = 1,
    page_size: int = 5,
    filter_dict: dict = None,
    file_type: str = "ALL",
) -> tuple:
    """
    List books with pagination from the database

    Args:
        session: session to connect to the database
        db: use in which database
        page: current page
        page_size: number of books per page
        filter_dict: filter to apply to the books
        file_type: type of file to filter

    Returns: metadata and data
    """
    filter_books = []
    if filter_dict:
        filter_books.append({"$match": filter_dict})
    if file_type != "ALL":
        filter_books.append({"$match": {"file_type": file_type}})
    filter_books.append(
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
        },
    )
    books = db.books.aggregate(
        filter_books,
        session=session,
    )
    books_data = list(books)
    if not books_data[0]["metadata"] or not books_data[0]["data"]:
        return None, None
    return books_data[0]["metadata"][0], books_data[0]["data"]


def print_books(
    *,
    session: pymongo.mongo_client.client_session,
    db: pymongo.mongo_client.database.Database,
    title: str,
    filter_dict: dict = None,
    file_type: str = "ALL",
):
    page = 1
    page_size = 5
    while True:
        print("-" * 79)
        print(title)
        print("-" * 79)
        metadata, data = list_book_pagination(
            session=session,
            db=db,
            page=page,
            page_size=page_size,
            filter_dict=filter_dict,
            file_type=file_type,
        )
        if metadata is None:
            print()
            print("No books found")
            print()
            print("-" * 79)
            print("1. Back to Main Menu")
            print("-" * 79)
            choice = get_choice("Enter your choice: ", 1)
            if choice == 1:
                break
            continue

        if data is None:
            print()
            print("No books found")
            print()
            print("-" * 79)
            print("1. Back to Main Menu")
            print("-" * 79)
            choice = get_choice("Enter your choice: ", 1)
            if choice == 1:
                break
            continue

        total_page = metadata["total_count"] // page_size
        if metadata["total_count"] % page_size != 0:
            total_page += 1
        print(f"Page {page} of {total_page}")

        for i in range(len(data)):
            print(f"{i+1}. {data[i]['title']}")

        i = len(data)
        if page < total_page:
            print(f"{i+1}. Next Page")

        if page > 1:
            if page == total_page:
                print(f"{i+1}. Previous Page")
                print(f"{i+2}. Back to Main Menu")
                print("-" * 79)
                choice = get_choice("Enter your choice: ", i + 2)
                if choice == i + 1:
                    page -= 1
                    continue
                elif choice == i + 2:
                    break
            else:
                print(f"{i+2}. Previous Page")
                print(f"{i+3}. Back to Main Menu")
                print("-" * 79)
                choice = get_choice("Enter your choice: ", i + 3)
                if choice == i + 1:
                    page += 1
                    continue
                elif choice == i + 2:
                    page -= 1
                    continue
                elif choice == i + 3:
                    break
        else:
            if page == total_page:
                print(f"{i+1}. Back to Main Menu")
                print("-" * 79)
                choice = get_choice("Enter your choice: ", i + 1)
                if choice == i + 1:
                    break
            else:
                print(f"{i+2}. Back to Main Menu")
                print("-" * 79)
                choice = get_choice("Enter your choice: ", i + 2)
                if choice == i + 1:
                    page += 1
                    continue
                elif choice == i + 2:
                    break
        book_data_menu(session=session, db=db, book_id=data[choice - 1]["_id"])


def search_books_by_title(
    *,
    session: pymongo.mongo_client.client_session,
    db: pymongo.mongo_client.database.Database,
):
    title = "Search by title"
    print("-" * 79)
    print(title)
    print("-" * 79)
    while True:
        search = input("Enter the search term: ")
        if search == "":
            print("Invalid input")
            continue
        break
    while True:
        file_type = input("Enter the file type (EPUB or PDF or ALL): ")
        if file_type not in ["EPUB", "PDF", "ALL"]:
            print("Invalid input")
            continue
        break
    filter_dict = {"title": {"$regex": search, "$options": "i"}}
    print_books(
        session=session,
        db=db,
        title=title,
        filter_dict=filter_dict,
        file_type=file_type,
    )


def search_books_by_author_name(
    *,
    session: pymongo.mongo_client.client_session,
    db: pymongo.mongo_client.database.Database,
):
    title = "Search by author name"
    print("-" * 79)
    print(title)
    print("-" * 79)
    while True:
        search = input("Enter the search term: ")
        if search == "":
            print("Invalid input")
            continue
        break

    while True:
        file_type = input("Enter the file type (EPUB or PDF or ALL): ")
        if file_type not in ["EPUB", "PDF", "ALL"]:
            print("Invalid input")
            continue
        break
    filter_dict = {
        "author": {"$elemMatch": {"name": {"$regex": search, "$options": "i"}}}
    }
    print_books(
        session=session,
        db=db,
        title=title,
        filter_dict=filter_dict,
        file_type=file_type,
    )


def search_books_by_author_pseudonym(
    *,
    session: pymongo.mongo_client.client_session,
    db: pymongo.mongo_client.database.Database,
):
    title = "Search by author pseudonym"
    print("-" * 79)
    print(title)
    print("-" * 79)
    while True:
        search = input("Enter the search term: ")
        if search == "":
            print("Invalid input")
            continue
        break
    while True:
        file_type = input("Enter the file type (EPUB or PDF or ALL): ")
        if file_type not in ["EPUB", "PDF", "ALL"]:
            print("Invalid input")
            continue
        break
    filter_dict = {
        "author": {"$elemMatch": {"pseudonym": {"$regex": search, "$options": "i"}}}
    }
    print_books(
        session=session,
        db=db,
        title=title,
        filter_dict=filter_dict,
        file_type=file_type,
    )


def search_books_by_genre(
    *,
    session: pymongo.mongo_client.client_session,
    db: pymongo.mongo_client.database.Database,
):
    title = "Search by genre"
    print("-" * 79)
    print(title)
    print("-" * 79)
    while True:
        search = input("Enter the search term: ")
        if search == "":
            print("Invalid input")
            continue
        break
    while True:
        file_type = input("Enter the file type (EPUB or PDF or ALL): ")
        if file_type not in ["EPUB", "PDF", "ALL"]:
            print("Invalid input")
            continue
        break
    filter_dict = {"genres": {"$regex": search, "$options": "i"}}
    print_books(
        session=session,
        db=db,
        title=title,
        filter_dict=filter_dict,
        file_type=file_type,
    )


def search_books_by_sub_genre(
    *,
    session: pymongo.mongo_client.client_session,
    db: pymongo.mongo_client.database.Database,
):
    title = "Search by sub-genre"
    print("-" * 79)
    print(title)
    print("-" * 79)
    while True:
        search = input("Enter the search term: ")
        if search == "":
            print("Invalid input")
            continue
        break
    while True:
        file_type = input("Enter the file type (EPUB or PDF or ALL): ")
        if file_type not in ["EPUB", "PDF", "ALL"]:
            print("Invalid input")
            continue
        break
    filter_dict = {"sub_genres": {"$regex": search, "$options": "i"}}
    print_books(
        session=session,
        db=db,
        title=title,
        filter_dict=filter_dict,
        file_type=file_type,
    )


def search_books_by_set_year(
    *,
    session: pymongo.mongo_client.client_session,
    db: pymongo.mongo_client.database.Database,
):
    title = "Search by set year"
    print("-" * 79)
    print(title)
    print("-" * 79)
    while True:
        search = input("Enter the search term: ")
        if search == "":
            print("Invalid input")
            continue
        break
    while True:
        file_type = input("Enter the file type (EPUB or PDF or ALL): ")
        if file_type not in ["EPUB", "PDF", "ALL"]:
            print("Invalid input")
            continue
        break
    filter_dict = {"set_year": {"$eq": int(search)}}
    print_books(
        session=session,
        db=db,
        title=title,
        filter_dict=filter_dict,
        file_type=file_type,
    )


def search_books_by_set_main_location(
    *,
    session: pymongo.mongo_client.client_session,
    db: pymongo.mongo_client.database.Database,
):
    title = "Search by set country"
    print("-" * 79)
    print(title)
    print("-" * 79)
    while True:
        search = input("Enter the search term: ")
        if search == "":
            print("Invalid input")
            continue
        break
    while True:
        file_type = input("Enter the file type (EPUB or PDF or ALL): ")
        if file_type not in ["EPUB", "PDF", "ALL"]:
            print("Invalid input")
            continue
        break
    filter_dict = {"set_main_location": {"$regex": search, "$options": "i"}}
    print_books(
        session=session,
        db=db,
        title=title,
        filter_dict=filter_dict,
        file_type=file_type,
    )


def search_books_by_main_character(
    *,
    session: pymongo.mongo_client.client_session,
    db: pymongo.mongo_client.database.Database,
):
    title = "Search by main character"
    print("-" * 79)
    print(title)
    print("-" * 79)
    while True:
        search = input("Enter the search term: ")
        if search == "":
            print("Invalid input")
            continue
        break
    while True:
        file_type = input("Enter the file type (EPUB or PDF or ALL): ")
        if file_type not in ["EPUB", "PDF", "ALL"]:
            print("Invalid input")
            continue
        break
    filter_dict = {"main_characters": {"$regex": search, "$options": "i"}}
    print_books(
        session=session,
        db=db,
        title=title,
        filter_dict=filter_dict,
        file_type=file_type,
    )


def search_books_menu(
    *,
    session: pymongo.mongo_client.client_session,
    db: pymongo.mongo_client.database.Database,
):
    print("-" * 79)
    print("Search for a book")
    print("-" * 79)
    print("1. Search by title")
    print("2. Search by author name")
    print("3. Search by author pseudonym")
    print("4. Search by genre")
    print("5. Search by sub-genre")
    print("6. Search by set year")
    print("7. Search by set country")
    print("8. Search by main character")
    print("9. Back to Main Menu")
    print("-" * 79)
    choice = get_choice("Enter your choice: ", 9)
    match choice:
        case 1:
            search_books_by_title(session=session, db=db)
        case 2:
            search_books_by_author_name(session=session, db=db)
        case 3:
            search_books_by_author_pseudonym(session=session, db=db)
        case 4:
            search_books_by_genre(session=session, db=db)
        case 5:
            search_books_by_sub_genre(session=session, db=db)
        case 6:
            search_books_by_set_year(session=session, db=db)
        case 7:
            search_books_by_set_main_location(session=session, db=db)
        case 8:
            search_books_by_main_character(session=session, db=db)
        case 9:
            pass


def main_menu():
    """
    Main Menu to interact with the user

    Returns: The choice the user made
    """
    print("-" * 79)
    print("Main Menu")
    print("-" * 79)
    print("1. Add a book")
    print("2. List all books")
    print("3. Search for a book")
    print("4. Exit")
    print("-" * 79)
    choice = get_choice("Enter your choice: ", 4)
    return choice


def main():
    """
    Main function to run the program

    Returns: EXIT_SUCCESS or EXIT_FAILURE
    """
    with (
        pymongo.MongoClient(URI) as client,
        client.start_session(causal_consistency=True) as session,
    ):
        db = client.get_database("books")

        try:
            while True:
                choice = main_menu()
                if choice == 1:
                    add_book_menu(session=session, db=db)
                elif choice == 2:
                    print_books(session=session, db=db, title="List all books")
                elif choice == 3:
                    search_books_menu(session=session, db=db)
                elif choice == 4:
                    print("Goodbye!")
                    break
        except KeyboardInterrupt:
            print("")
            print("Goodbye!")
    return EXIT_SUCCESS


EXIT_SUCCESS = 0
EXIT_FAILURE = 1
URI = "mongodb://localhost:27017/"
if __name__ == "__main__":
    SystemExit(main())
