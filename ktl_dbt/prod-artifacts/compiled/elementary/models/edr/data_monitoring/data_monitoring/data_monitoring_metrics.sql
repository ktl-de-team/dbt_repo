


    
    
        
    
    select * from (
            select
            
                
        cast('dummy_string' as string) as id

,
                
        cast('dummy_string' as string) as full_table_name

,
                
        cast('dummy_string' as string) as column_name

,
                
        cast('dummy_string' as string) as metric_name

,
                
        cast(123456789.99 as float) as metric_value

,
                
        cast('dummy_string' as string) as source_value

,
                cast('2091-02-17' as timestamp) as bucket_start

,
                cast('2091-02-17' as timestamp) as bucket_end

,
                
        cast(123456789 as integer) as bucket_duration_hours

,
                cast('2091-02-17' as timestamp) as updated_at

,
                
        cast('dummy_string' as string) as dimension

,
                
        cast('dummy_string' as string) as dimension_value

,
                
        cast('dummy_string' as string) as metric_properties

,
                cast('2091-02-17' as timestamp) as created_at


        ) as empty_table
        where 1 = 0
