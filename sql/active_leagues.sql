update public.football_app_league
    set level = case when id in (39, 61, 78, 135, 88, 94, 235, 140, 203) then 1 when id in (40, 62, 79, 136, 89, 95, 236, 141, 204) then 2 else 3 end,
        is_active = case when id in (39, 40, 61, 62, 78, 79, 135, 136, 88, 89, 94, 95, 235, 236, 140, 141, 203, 204) then true else false end
where true;