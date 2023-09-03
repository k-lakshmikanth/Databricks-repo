from databricks.sdk.runtime import *

# This is the config file any parameter can be changed here. 
KeyVaultScopeName = 'KeyVault'
KeyVaultSecretName = 'Databricks-spn-client-secret'
config = {
# 'workspace url' This can be found in the url bar.
  "workspace_url":"https://adb-4858399918092056.16.azuredatabricks.net",

# 'client id' is the application (client) id of the SPN
  "client_id" : "78fe5a65-b9a4-4118-940f-5d0461a57afb",

# 'client secret' the secret value generated in SPN which is stored in key vault
  "client_secret" : dbutils.secrets.get(KeyVaultScopeName,KeyVaultSecretName),

# 'tenet id' is found in the SPN
  "tenet_id" : "c12aba17-76aa-43ae-a98e-54302f617a40" 
}