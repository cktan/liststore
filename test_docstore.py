import unittest
import calendar
import os, sys, time
import docstore

class Conf:
    bucketname = None
    redis_host = None
    redis_port = None
    aws_access_key = None
    aws_secret_key = None


def test_docstore():

    if not os.environ.get('AWS_ACCESS_KEY'):
        sys.exit('AWS_ACCESS_KEY not set')
    if not os.environ.get('AWS_SECRET_KEY'):
        sys.exit('AWS_SECRET_KEY not set')

    Conf.bucketname = 'my-dumping-grounds'
    Conf.redis_host = 'localhost'
    Conf.redis_port = 6379
    Conf.aws_access_key = os.environ['AWS_ACCESS_KEY']
    Conf.aws_secret_key = os.environ['AWS_SECRET_KEY']

    ds = docstore.DocStore(Conf.bucketname,
                           Conf.aws_access_key, Conf.aws_secret_key,
                           Conf.redis_host, Conf.redis_port)

    path = 'test-docstore/dummy'

    # remove all 
    for i in ds.list(path, 1000):
        ds.delete(path, i)

    # check that it is indeed empty
    residue = ds.list(path, 1000)
    assert residue == [], 'List is not empty'

    # try to read ... should not be able to.
    for i in xrange(27):
        s = ds.get(path, str(i))
        assert s == None, 'Unexpected content'

    # put in 27 entries
    for i in xrange(27):
        ds.put(path, str(i), 'this is ' + str(i))

    # read all 27 entries
    for i in xrange(27):
        s = ds.get(path, str(i))
        assert s == 'this is ' + str(i), 'Content mismatch'

    # delete even entries from cache
    for i in xrange(0, 27, 2):
        ds._deleteFromCache(path, str(i))

    # read again. the even entries will have to hit s3.
    for i in xrange(27):
        s = ds.get(path, str(i))
        assert s == 'this is ' + str(i), 'Content mismatch'


