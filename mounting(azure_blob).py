storageAccountName = "adlskrishna23"
storageAccountAccessKey = "1GrP5UwK+b9YIgiokMZti64d3buVAPcYaH41Kmx7uVF0+mctWZaH5xR7StWnFUukDleUvDZ9uFLd+AStlYtOUg=="
blobContainerName = "source"
mountPoint = "/mnt/source_adls2/"
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
