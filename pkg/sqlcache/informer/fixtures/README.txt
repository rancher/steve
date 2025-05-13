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

Some sample queries. Note that you might need to rewrite a query to
have no newlines depending on how you feed it to sqlite.

#1.

SELECT o.object, ext1.country, ext1."foodCode" FROM "_v1_Namespace" o
  JOIN "_v1_Namespace_fields" f ON o.key = f.key
  JOIN "_v1_Foods_fields" ext1 ON f."metadata.fields[2]" = ext1."foodCode"
  ORDER BY ext1."country" ASC NULLS LAST


#2.

SELECT __ix_object, __ix_f_metadata_state_name, __ix_ext2_spec_clusterName, __ix_f_metadata_name, __ix_dekid FROM (
  SELECT DISTINCT o.object AS __ix_object, o.objectnonce AS __ix_objectnonce, o.dekid AS __ix_dekid, ext2."spec.clusterName" AS __ix_ext2_spec_clusterName, f."metadata.state.name" AS __ix_f_metadata_state_name, f."metadata.name" AS __ix_f_metadata_name FROM "_v1_Namespace" o
    JOIN "_v1_Namespace_fields" f ON o.key = f.key
    LEFT OUTER JOIN "_v1_Namespace_labels" lt1 ON o.key = lt1.key
    JOIN "management.cattle.io_v3_Project_fields" ext2 ON lt1.value = ext2."metadata.name"
    WHERE ((f."metadata.name" LIKE "%cluster-%" ESCAPE '\') OR (f."metadata.name" LIKE "%cattle-%" ESCAPE '\')) AND
      (lt1.label = "field.cattle.io/projectId")

UNION ALL
  SELECT DISTINCT o.object AS __ix_object, o.objectnonce AS __ix_objectnonce, o.dekid AS __ix_dekid, NULL AS __ix_ext2_spec_clusterName, NULL AS __ix_f_metadata_state_name, NULL AS __ix_f_metadata_name FROM "_v1_Namespace" o
    JOIN "_v1_Namespace_fields" f ON o.key = f.key
    LEFT OUTER JOIN "_v1_Namespace_labels" lt1 ON o.key = lt1.key
    WHERE ((f."metadata.name" LIKE "%cluster-%" ESCAPE '\') OR (f."metadata.name" LIKE "%cattle-%" ESCAPE '\')) AND
      (o.key NOT IN (SELECT o1.key FROM "_v1_Namespace" o1
		JOIN "_v1_Namespace_fields" f1 ON o1.key = f1.key
		LEFT OUTER JOIN "_v1_Namespace_labels" lt1i1 ON o1.key = lt1i1.key
		WHERE lt1i1.label = "field.cattle.io/projectId"))

)
  ORDER BY __ix_f_metadata_state_name ASC, __ix_ext2_spec_clusterName ASC NULLS LAST, __ix_f_metadata_name ASC


