Debugging aids.

To use this data in sqlite:

```
$ cd .../fixtures # (this directory)
$ rm -f test.db
$ sqlite3 test.db
sqlite> .read schema.txt
sqlite> .read _v1_Namespace.txt
sqlite> .read _v1_Namespace_fields.txt
sqlite> .read _v1_Namespace_labels.txt
sqlite> .read management.cattle.io_v3_Project.txt
sqlite> .read management.cattle.io_v3_Project_fields.txt
sqlite> .read management.cattle.io_v3_Project_labels.txt
sqlite> .read _v1_Foods.txt
sqlite> .read _v1_Foods_fields.txt
sqlite> .read _v1_Foods_labels.txt
```

And query the data to your heart's content.

