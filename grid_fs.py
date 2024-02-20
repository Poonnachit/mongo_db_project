#! /usr/bin/env python3
"""
bindaddy.py - Demo program showing how to use pymongo's gridfs module with MongoDB
"""
import os
import stat
import datetime
import pymongo
import gridfs

class LengthError(Exception):
    """ file length is invalid """

class BadFile(Exception):
    """ File is invalid """

def fileio_test(*,client,session,fname):
    """ Read in fname and write it back out again """
    filter = {"filename": fname}
    # select mydb database
    db = client.get_database("mydb")
    # get access to mydb's gridfs area
    fs = gridfs.GridFS(db)
    # if the file is already in the gridfs
    if fs.exists(filter, session=session):
        # find the gridfs _id
        the_id = fs.find_one(filter, no_cursor_timeout=True, session=session)._id
        # and use it to delete the file from the gridfs
        fs.delete(the_id, session=session)
        # delete 'the_id' variable so we don't accidentally use a stale _id value
        del the_id
    if os.path.isdir(fname):
        raise BadFile("specified file \"{fname}\" is a directory, not a file.")
    # open the file on disk for binary read
    with open(fname,'rb') as infile:
        # load the file's data into gridfs, saving the generated _id in 'the_id'
        the_id = fs.put(infile.read(),
                        filename=fname,
                        content_type='image/File',
                        comment='Two ladies wearing cheongsam (qipao) dresses',
                        session=session)
    # search for the file we just loaded by name in gridfs
    with fs.find(filter, no_cursor_timeout=True, session=session) as cur:
        # for each match we find, show some information 
        for tfile in cur:
            print(f"_id={tfile._id}")
            print(f"name=\"{tfile.name}\"")
            print(f"length in bytes={tfile.length}")
            print(f"metadata={tfile.metadata}")
    # output filename will be original name with '.copy1' appended to it
    outfname = fname + os.path.extsep + "copy1"
    # if the output file already exists, remove it
    try:
        os.remove(outfname)
    except FileNotFoundError:
        pass # ignore not found errors
    # open a outfname file on disk for binary write
    with open(outfname, 'wb') as outfile:
        # and copy the data from gridfs to that new file
        outfile.write(fs.get(the_id, session=session).read())
    # make that new file read-only
    os.chmod(outfname, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
    # fetch latest version of a file by name
    tfile = fs.get_last_version(filename=fname, session=session)
    _id, filename, filelength, upload_date = tfile._id, tfile.name, tfile.length, tfile.upload_date
    content_type, metadata, comment = tfile.content_type, tfile.metadata, tfile.comment
    first_two_bytes = tfile.read(2).hex().upper()
    tfile.seek(-2,2)
    last_two_bytes = tfile.read(2).hex().upper()
    tfile.close()
    print(f"_id={_id}\nname=\"{filename}\"\nlength in bytes={filelength}")
    print(f"upload date={upload_date}\ncontent type={content_type}")
    print(f"metadata={metadata}\ncomment={comment}")
    if filelength != os.path.getsize(fname):
         raise LengthError("Bad length, got {filelength}, expected {os.path.getsize(fname)}")
    if first_two_bytes != 'FFD8':
         raise BadFile("Bad SOI marker, got {first_two_bytes}, expected 'FFD8'")
    if last_two_bytes != 'FFD9':
         raise BADFile("Bad EOI marker, got {last_two_bytes}, expected 'FFD9'")

def main():
    """ Main entry point """
    retcode = EXIT_SUCCESS # hope for the best
    # connect to MongoDB server and start a session
    with pymongo.MongoClient(host='127.0.0.1', port=27017) as client, \
        client.start_session(causal_consistency=True) as session:
        try:
            fileio_test(client=client, session=session, fname=INPUT_FILENAME)
        except (BadFile, LengthError) as excpn:
            print(excpn)
            retcode = EXIT_FAILURE # trouble ....
    return retcode

EXIT_SUCCESS=0
EXIT_FAILURE=1
INPUT_FILENAME = "cheongsam-qipao.jpg"

if __name__ == "__main__":
     raise SystemExit(main())