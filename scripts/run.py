import os
from src.edb.dbhelper import DbConnector
from src.edb.githelper import RepoReader

dbconfig = {'user':'root', 'pass':'S!mpl3P4ssw0rd', 'host': 'mariadb', 'db': 'edb2'}
if os.environ.get('AIRFLOW__CORE__EXECUTOR') is None:
    dbconfig['host'] = "localhost"

myconn = DbConnector(dbconfig)
# val = [('PeterXXXX', 'Lowstreet 4', 22),
#        ('AmyXXXX', 'Apple st 652', 22) ]

# myconn.insert('test', ['name','aaa','bbb'], val)


# obj = RepoReader(org='ahhaur', repo='ex_danish_bread')
# # obj.main()
# obj.test_script()
exit(0)