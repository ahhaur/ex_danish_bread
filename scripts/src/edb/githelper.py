import requests
import time
from datetime import date, datetime
from dateutil.relativedelta import relativedelta

class RepoReader:
    API_URL = "https://api.github.com"
    REQ_BUFFER = 90  # request buffer in second
    PAGE_SIZE = 20  # commit per request

    def __init__(self, org=None, repo=None):
        if org is None or repo is None:
            print("Org or Repo is not defined.")
            exit(1)

        self.repo_url = f"{self.API_URL}/repos/{org}/{repo}"
        start_ts = date.today() + relativedelta(months=-6)
        self.start_date = start_ts.strftime("%Y-%m-%dT%H:%M:%SZ")

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

            retval.append({
                'sha': r['sha'],
                'author': r['commit']['author']['name'],
                'author_date': r['commit']['author']['date'],
                'committer': r['commit']['committer']['name'],
                'commit_date': r['commit']['committer']['date'],
                'msg': r['commit']['message'].split('\n', 1)[0]
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