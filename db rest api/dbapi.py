from requests import get,post,patch,delete
from config import *

# 'databricks_api' class gets the access token and stored in header attribute which can be used for further classes
class databricks_api():
    def __init__(self):
        self.__dict__.update(config)
        self.get_token()
    
    def get_token(self):
        responce = post(f"https://login.microsoftonline.com/{config['tenet_id']}/oauth2/token",\
            headers={"Content-type": "application/x-www-form-urlencoded"},
                data={
                    "grant_type": "client_credentials",
                    "client_id": config['client_id'],
                    "client_secret": config['client_secret'],
                    "resource": "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"
                })
        self.header={"Authentication":f'Bearer {responce.json()["access_token"]}'}
    

# "repos" class is used for multiple functions like 'repos list', 'repo metadata', 'create repo', 'update repo', 'delete repo', 'repo permissions data'
class repos(databricks_api):
    def __init__(self):
        super().__init__()
    
    def get_repos_list(self) -> dict:
        responce = get(f"{self.workspace_url}/api/2.0/repos",headers=self.header)
        return responce.json()
    
    def get_repo(self,repo_id):
        responce = get(f"{self.workspace_url}/api/2.0/repos/{repo_id}",headers=self.header)
        return responce.json()
    
    def create_repo(self,url,provider='github',path="/Repos"):
        path =f"{path}/{url.split('/')[-1]}" if path.split('/')[-1] != url.split('/')[-1] else path
        responce = post(f"{self.workspace_url}/api/2.0/repos",headers=self.header,\
                        json={"url":url,"provider":provider,"path":path})
        return responce.json()

    def update_repo(self,repo_id,branch):
        responce = patch(f"{self.workspace_url}/api/2.0/repos/{repo_id}/branch",headers=self.header,\
                        data={"branch":branch})
        return responce.json()

    def delete_repo(self,repo_id):
        responce = delete(f"{self.workspace_url}/api/2.0/repos/{repo_id}",headers=self.header)
        print("Repo deleted successfully") if responce.status_code == 200 else print("Repo deletion failed")
        if responce.status_code != 200:
            return responce.json()
    
    def get_repo_permissions(self,repo_id):
        responce = get(f"{self.workspace_url}/api/2.0/permissions/repos/{repo_id}",headers=self.header)
        return responce.json()