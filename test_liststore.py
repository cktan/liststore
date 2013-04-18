import calendar
import os, sys, time
import liststore

class Conf:
    bucketname = None
    redis_host = None
    redis_port = None
    aws_access_key = None
    aws_secret_key = None


def test_liststore():
    if not os.environ.get('AWS_ACCESS_KEY'):
        sys.exit('AWS_ACCESS_KEY not set')
    if not os.environ.get('AWS_SECRET_KEY'):
        sys.exit('AWS_SECRET_KEY not set')

    Conf.bucketname = 'my-dumping-grounds'
    Conf.redis_host = 'localhost'
    Conf.redis_port = 6379
    Conf.aws_access_key = os.environ['AWS_ACCESS_KEY']
    Conf.aws_secret_key = os.environ['AWS_SECRET_KEY']

    ls = liststore.ListStore(Conf.bucketname,
                             Conf.aws_access_key, Conf.aws_secret_key,
                             Conf.redis_host, Conf.redis_port)

    name = 'test-liststore'
    start = calendar.timegm(time.strptime('20130101', '%Y%m%d'))
    feb14 = calendar.timegm(time.strptime('20130214', '%Y%m%d'))
    mar31 = calendar.timegm(time.strptime('20130331', '%Y%m%d'))
    jan10 = calendar.timegm(time.strptime('20130110', '%Y%m%d'))
    jun1 = calendar.timegm(time.strptime('20130601', '%Y%m%d'))
    mar14 = calendar.timegm(time.strptime('20130314', '%Y%m%d'))
    aug23 = calendar.timegm(time.strptime('20130823', '%Y%m%d'))

    # fresh start for test
    ls.deleteName(name)

    # insert one item per day for the whole year in batches of 1, 2, 4, 8, 16, 64
    def doInsert():
        i = 0
        while i < 365:
            out = []
            for j in xrange(1 << (i % 7)):
                if i >= 365: break
                t = start + i * (24 * 60 * 60)
                out += [(t, 'hello ' + time.asctime(time.gmtime(t)))]
                i = i + 1
            if out:
                ls.append(name, out)

    def doDelete():
        # delete all
        for i in xrange(365):
            ls.delete(name, start + i * (24 * 60 * 60))

    def doDismiss():
        # print 'dismiss March 31'
        ls.setDismissed(name, mar31, prior=False)
        r = ls.retrieve(name, mar31)
        assert r == None, 'Dismissed record is not dismissed'

        # print 'dismiss everything on and before Feb 14'
        ls.setDismissed(name, feb14, prior=True)
        r = ls.retrieve(name, feb14)
        assert r == None, 'Dismissed record is not dismissed'
        r = ls.retrieve(name, jan10)
        assert r == None, 'Dismissed record is not dismissed'

    def doSetSeen():
        # print 'set seen on June 1'
        ls.setSeen(name, jun1, prior=False)
        r = ls.retrieve(name, jun1)
        assert (r and r['seen']), 'Seen record is not seen'

        # print 'set everything seen on and before March 14'
        ls.setSeen(name, mar14, prior=True)

    def verifySeenAndDismissed():
        c = ls.count(name)
        assert c['total'] == 365, 'Wrong total count %d' % c['total']
        assert c['seen'] == 74, 'Wrong seen count %d' % c['seen']
        assert c['dismissed'] == 46, 'Wrong dismissed count %d' % c['dismissed']
        for i in xrange(365):
            t = start + i * 24 * 60 * 60
            r = ls.retrieve(name, t)

            if t <= feb14 or t == mar31:
                assert r == None, 'Dismissed record is not dismissed'
                continue

            assert r != None, 'Non-dismissed record is not found'

            if t <= mar14 or t == jun1:
                assert r['seen'], 'Seen record is not seen'
                continue

            assert not r['seen'], 'Not-seen record is seen'


    def doReverseScan():
        out = ls.reverseScan(name, aug23, limit=300)
        assert out[-1]['ctime'] == feb14 + 24 * 60 * 60, 'last record should be feb15'

        t = aug23
        for r in out:
            if t == mar31:
                t = t - 24 * 60 * 60
            assert r['ctime'] == t, 'Expected ctime of %s but got %s' % (t, r['ctime'])
            if t <= mar14 or t == jun1:
                assert r['seen'], 'Seen record is not seen'
            else:
                assert not r['seen'], 'Not-seen record is seen'
            t = t - 24 * 60 * 60


    # print 'test insert'
    doInsert()

    if False:
        # print 'test delete'
        doDelete()
        # print 'redo insert'
        doInsert()

    # print 'test dismiss'
    doDismiss()
    # print 'test seen'
    doSetSeen()
    # print 'verifying ...'
    verifySeenAndDismissed()
    # print 'clear cache'
    ls.clearCache(name)
    # print 'verifying again ...'
    verifySeenAndDismissed()

    # print 'test reverse scan'
    doReverseScan()

    # print 'clear cache and test again'
    ls.clearCache(name)

    # print 'test reverse scan again'
    doReverseScan()

