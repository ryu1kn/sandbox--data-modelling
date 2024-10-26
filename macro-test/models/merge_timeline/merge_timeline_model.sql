{{ macro_lib.merge_timeline(
    relation=source('src_merge_timeline_model', 'numbers'),
    partition_by=['id', 'category', 'val'],
) }}
