import requests
import time
import pytz
from datetime import date, datetime
from dateutil.relativedelta import relativedelta

class RepoReader:
    API_URL = "https://api.github.com"
    REQ_BUFFER = 90  # request buffer in second
    PAGE_SIZE = 100  # commit per request

    def __init__(self, org=None, repo=None, start_date=None):
        if org is None or repo is None:
            print("Org or Repo is not defined.")
            exit(1)

        self.repo_url = f"{self.API_URL}/repos/{org}/{repo}"
        start_ts = date.today() + relativedelta(months=-1) if start_date is None else start_date
        self.start_date = start_ts.strftime("%Y-%m-%dT%H:%M:%SZ")

        print(f'Start date is {start_ts}')

    def main(self):
        print('Getting branches info...')
        branches = self.get_branch()

        retval = {}
        for i, branch in enumerate(branches):
            retval = {branch['name']: self.get_branch_commit(branch['name'], branch['sha'])}
            if i > 0:
                break

        return retval

    def test_script(self):
        print("this is working...")
    
    # get all commits for branch
    def get_branch_commit(self, branch_name, branch_sha):
        self.buffer()
        print(f'Getting all commits for #{branch_name}...')
        retval = []
        sha = branch_sha
        commits = self.get_commit(sha=sha, per_page=self.PAGE_SIZE)
        while len(commits) > 0:
            print("SHA: {} \nTotal record: {}".format(sha, len(commits)))
            for commit in commits:
                sha = commit['sha']
                commit['branch_name'] = branch_name
                retval.append(commit)
                print(commit, "\n", "-"*50)
            
            if len(commits) < self.PAGE_SIZE-1:
                print('Records lesser than expected number, job done.')
                break

            self.buffer()
            commits = self.get_commit(sha=sha, skip_duplicate=True, per_page=self.PAGE_SIZE)

        return retval

    # get all commits for branch
    def get_branch_commit_conn(self, branch_name, branch_sha, conn=None, tpl=None, skip_first=False):
        self.buffer()
        print(f'Getting all commits for #{branch_name}...')
        if conn is not None:
            col = [_ for _ in tpl]
        
        sha = branch_sha
        commits = self.get_commit(sha=sha, skip_duplicate=skip_first, per_page=self.PAGE_SIZE)
        retval = []
        while len(commits) > 0:
            print("SHA: {} \nTotal record: {}".format(sha, len(commits)))
            retval = []
            for commit in commits:
                sha = commit['sha']
                tmp = tpl
                tmp['commit_sha'] = commit['sha']
                tmp['author_id'] = commit['author_id']
                tmp['author_login'] = commit['author_login']
                tmp['author_name'] = commit['author_name']
                tmp['committer_id'] = commit['committer_id']
                tmp['committer_login'] = commit['committer_login']
                tmp['committer_name'] = commit['committer_name']
                tmp['commit_date'] = commit['commit_date']
                tmp['commit_datetime'] = self.convert_timestamp(commit['commit_date'])
                tmp['message'] = commit['message']  #.encode('utf8')

                if conn is not None:
                    retval.append(tuple(tmp.values()))

                # print(tmp, "\n", "-"*50)
            
            if conn is not None:
                conn.insert_batch('commit_table', col, retval)
            
            if len(commits) < self.PAGE_SIZE-1:
                print('Records lesser than expected number, job done.')
                break

            self.buffer()
            commits = self.get_commit(sha=sha, skip_duplicate=True, per_page=self.PAGE_SIZE)

        return retval

    def convert_timestamp(self, ts_str):
        source_format = '%Y-%m-%dT%H:%M:%SZ'
        insert_format = '%Y-%m-%d %H:%M:%S'
        return datetime.strptime(ts_str, source_format).replace(tzinfo=pytz.UTC).strftime(insert_format)

    # get commit list
    def get_commit(self, sha=None, skip_duplicate=False, per_page=100):
        uri = {
            "sha": sha,
            "per_page": per_page,
            "page": 1,
            "since": self.start_date
        }

        url = "{}/commits?{}".format(self.repo_url, "&".join([f"{_}={uri[_]}" for _ in uri]))
        res = self.request(url)
        retval = []
        for r in res:
            if  skip_duplicate and r['sha'] == sha:
                continue
            
            tmp_author = r.get('author')
            tmp_commit = r.get('committer')
            tmp_author_name = r['commit']['author']['name']
            tmp_commit_name = r['commit']['committer']['name']

            retval.append({
                'sha': r['sha'],
                'author_date': r['commit']['author']['date'],
                'author_name': tmp_author_name,
                'author_login': tmp_author_name if tmp_author is None else tmp_author['login'],
                'author_id': 0 if tmp_author is None else tmp_author['id'],
                'commit_date': r['commit']['committer']['date'],
                'committer_name': tmp_commit_name,
                'committer_login': tmp_commit_name if tmp_commit is None else tmp_commit['login'],
                'committer_id': 0 if tmp_commit is None else tmp_commit['id'],
                'message': r['commit']['message'].split('\n', 1)[0]
            })

        return retval

    # get branch list
    def get_branch(self):
        res = self.request(f"{self.repo_url}/branches")
        return [{'name': _['name'], 'sha': _['commit']['sha']} for _ in res]

    # API request 
    def request(self, url=None):
        try:
            res = requests.get(url)
        except Exception as ex:
            print(ex)
        else:
            retval = res.json()
            if res.status_code == 200 and isinstance(retval, list):
                return retval
            else:
                print(retval)
        
        return []
    
    def buffer(self):
        # Set request buffer to prevent rate-limiting
        print(f"Wait for {self.REQ_BUFFER}s...")
        time.sleep(self.REQ_BUFFER)


if __name__ == '__main__':

    reader = RepoReader(org='apache', repo='airflow')
    results = reader.main()