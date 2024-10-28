{% macro reslice_timeline(relation, partition_by) %}
{% set partition_by_str = partition_by | join(', ') %}
with
    validity_event_ts as (
        select * from (
            select
                valid_from as timeline_ts,
                {{ partition_by_str }}
            from {{ relation }}
            union   -- Use union to deduplicate
            select
                -- To later distinguish null as the "current" from null as "not exist"
                coalesce(valid_to, '9999-12-31') as timeline_ts,
                {{ partition_by_str }}
            from {{ relation }}
        )
        order by timeline_ts
    ),
    timeline as (
        select
            timeline_ts as valid_from,
            lead(timeline_ts) over w as valid_to,
            {{ partition_by_str }},
        from validity_event_ts
        window w as (partition by {{ partition_by_str }} order by timeline_ts)
        qualify valid_to is not null    -- Here drop "not exist" time period
    ),
    records_re_sliced_w_new_timeline as (
        select
            orig.* exclude (valid_from, valid_to),
            timeline.valid_from,
            timeline.valid_to,
        from timeline
        left join {{ relation }} as orig
        on true
            {% for p in partition_by %}
            and orig.{{ p }} = timeline.{{ p }}
            {% endfor %}
            and orig.valid_from <= timeline.valid_from
            and orig.valid_to >= timeline.valid_to
    )
select * from records_re_sliced_w_new_timeline
{% endmacro %}
