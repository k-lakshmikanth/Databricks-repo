# Databricks notebook source
storageAccountName = "adlskrishna2"
storageAccountAccessKey = "~sNKbFvGTr1dUw8nhD5QTUD8YT0+aLuAiBYgmvWGqqSfZxmisvp2O0KRcArO1Cz/k2UUxdzbeIxSZ+ASty4e3jA=="
blobContainerName = "auto-test"
mountPoint = "/mnt/auto-test/"
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
  try:
    dbutils.fs.mount(
      source = "wasbs://{}@{}.blob.core.windows.net".format(blobContainerName, storageAccountName),
      mount_point = mountPoint,
      extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey}
    )
    print("mount succeeded!")
  except Exception as e:
    print("mount exception", e)


# COMMAND ----------

# MAGIC %fs ls /mnt/auto-test/
