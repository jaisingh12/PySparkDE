from pyspark.sql.functions import lit, current_date

target_schema = ['empid', 'name', 'place', 'salary', 'eff_start_date', 'eff_end_date', 'flag']

target_data = [ (100, 'Unchanged-Emp', 'Delhi', 10000, "2024-01-01", None , 1),
                (200, 'Obsolete-Emp', 'Pune', 20000,  "2024-01-01", "", 1 )]

source_schema = ['empid', 'name', 'place', 'salary']
source_data = [ (300, 'New-Emp', 'Agra', 15000 ),      # New Employee
                (200, 'Updated-Emp', 'Pune', 25000 )]  # Salary increased 

target_df  = spark.createDataFrame(target_data, target_schema)
source_df  = spark.createDataFrame(source_data, source_schema)
target_df.show(truncate=False)
source_df.show(truncate=False)

'''
+-----+-------------+-----+------+--------------+------------+----+
|empid|name         |place|salary|eff_start_date|eff_end_date|flag|
+-----+-------------+-----+------+--------------+------------+----+
|100  |Unchanged-Emp|Delhi|10000 |2024-01-01    |null        |1   |
|200  |Obsolete-Emp |Pune |20000 |2024-01-01    |            |1   |
+-----+-------------+-----+------+--------------+------------+----+

+-----+-----------+-----+------+
|empid|name       |place|salary|
+-----+-----------+-----+------+
|300  |New-Emp    |Agra |15000 |
|200  |Updated-Emp|Pune |25000 |
+-----+-----------+-----+------+

# required SCD-2 result
+-----+-------------+-----+------+--------------+------------+----+
|empid|name         |place|salary|eff_start_date|eff_end_date|flag|
+-----+-------------+-----+------+--------------+------------+----+
|100  |Unchanged-Emp|Delhi|10000 |2024-01-01    |null        |1   |
|200  |Obsolete-Emp |Pune |20000 |2024-01-01    |2024-02-01  |0   |
|200  |Updated-Emp  |Pune |25000 |2024-02-01    |null        |1   |
|300  |New-Emp      |Agra |15000 |2024-02-01    |null        |1   |
+-----+-------------+-----+------+--------------+------------+----+

'''


# new records
new_df = source_df.join(target_df, 'empid', 'left_anti')
new_df.show()
'''
+-----+-------+-----+------+
|empid|   name|place|salary|
+-----+-------+-----+------+
|  300|New-Emp| Agra| 15000|
+-----+-------+-----+------+
'''

cond = [target_df.empid == source_df.empid ]
base_df = target_df.join(source_df, cond, 'left')
base_df.show()
'''
+-----+-------------+-----+------+--------------+------------+----+-----+-----------+-----+------+
|empid|         name|place|salary|eff_start_date|eff_end_date|flag|empid|       name|place|salary|
+-----+-------------+-----+------+--------------+------------+----+-----+-----------+-----+------+
|  100|Unchanged-Emp|Delhi| 10000|    2024-01-01|        null|   1| null|       null| null|  null|
|  200| Obsolete-Emp| Pune| 20000|    2024-01-01|            |   1|  200|Updated-Emp| Pune| 25000|
+-----+-------------+-----+------+--------------+------------+----+-----+-----------+-----+------+
'''


# filter unchanged records
unchanged_df = base_df.filter(source_df.empid.isNull()).select(target_df['*'])
unchanged_df.show()
'''
+-----+-------------+-----+------+--------------+------------+----+
|empid|         name|place|salary|eff_start_date|eff_end_date|flag|
+-----+-------------+-----+------+--------------+------------+----+
|  100|Unchanged-Emp|Delhi| 10000|    2024-01-01|        null|   1|
+-----+-------------+-----+------+--------------+------------+----+
'''


# identify updated records for new entry
updated_df = base_df.filter(source_df.empid.isNotNull()).select(source_df['*'])
updated_df.show()

# identify updated records for obsolete entry
obsolete_df = base_df.filter(source_df.empid.isNotNull()).select(target_df['*'])
obsolete_df.show()
'''
+-----+-----------+-----+------+
|empid|       name|place|salary|
+-----+-----------+-----+------+
|  200|Updated-Emp| Pune| 25000|
+-----+-----------+-----+------+

+-----+------------+-----+------+--------------+------------+----+
|empid|        name|place|salary|eff_start_date|eff_end_date|flag|
+-----+------------+-----+------+--------------+------------+----+
|  200|Obsolete-Emp| Pune| 20000|    2024-01-01|            |   1|
+-----+------------+-----+------+--------------+------------+----+
'''


# identify updated records for obsolete entry
obsolete_df = obsolete_df.withColumn("eff_end_date", current_date()) \
                         .withColumn("flag", lit(0))
obsolete_df.show()
'''
+-----+------------+-----+------+--------------+------------+----+
|empid|        name|place|salary|eff_start_date|eff_end_date|flag|
+-----+------------+-----+------+--------------+------------+----+
|  200|Obsolete-Emp| Pune| 20000|    2024-01-01|  2024-02-01|   0|
+-----+------------+-----+------+--------------+------------+----+
'''


# union : new & updated records and add scd2 meta-deta 
delta_df = new_df.union(updated_df)
delta_df = delta_df.withColumn("eff_start_date", current_date()) \
                    .withColumn("eff_end_date", lit(None)) \
                    .withColumn("flag", lit(1)) 
        
delta_df.show()
'''
+-----+-----------+-----+------+--------------+------------+----+
|empid|       name|place|salary|eff_start_date|eff_end_date|flag|
+-----+-----------+-----+------+--------------+------------+----+
|  300|    New-Emp| Agra| 15000|    2024-02-01|        null|   1|
|  200|Updated-Emp| Pune| 25000|    2024-02-01|        null|   1|
+-----+-----------+-----+------+--------------+------------+----+
'''


# union all datasets : delta_df + obsolete_df + unchanged_df
scd2_df = unchanged_df.union(delta_df).union(obsolete_df)
scd2_df.sort('empid','name').show(20,0)
'''
+-----+-------------+-----+------+--------------+------------+----+
|empid|name         |place|salary|eff_start_date|eff_end_date|flag|
+-----+-------------+-----+------+--------------+------------+----+
|100  |Unchanged-Emp|Delhi|10000 |2024-01-01    |null        |1   |
|200  |Obsolete-Emp |Pune |20000 |2024-01-01    |2024-02-01  |0   |
|200  |Updated-Emp  |Pune |25000 |2024-02-01    |null        |1   |
|300  |New-Emp      |Agra |15000 |2024-02-01    |null        |1   |
+-----+-------------+-----+------+--------------+------------+----+
'''
