


    select * from (
            select
            
                
        cast('dummy_string' as string) as column_state_id

,
                
        cast('dummy_string' as string) as full_column_name

,
                
        cast('dummy_string' as string) as full_table_name

,
                
        cast('dummy_string' as string) as column_name

,
                
        cast('dummy_string' as string) as data_type

,
                
        cast (True as boolean) as is_new

,
                cast('2091-02-17' as timestamp) as detected_at

,
                cast('2091-02-17' as timestamp) as created_at


        ) as empty_table
        where 1 = 0
