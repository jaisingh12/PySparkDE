schema = ['empid', 'name', 'place', 'salary']

target_data = [ (100, 'Unchanged-Emp', 'Delhi', 10000 ),
                (200, 'Obsolete-Emp', 'Pune', 20000 )]

source_data = [ (300, 'New-Emp', 'Agra', 15000 ),      # New Employee
                (200, 'Updated-Emp', 'Pune', 25000 )]  # Salary increased 

target_df  = spark.createDataFrame(target_data, schema)
source_df  = spark.createDataFrame(source_data, schema)

target_df.show(truncate=False)
source_df.show(truncate=False)

'''
+-----+-------------+-----+------+
|empid|name         |place|salary|
+-----+-------------+-----+------+
|100  |Unchanged-Emp|Delhi|10000 |
|200  |Obsolete-Emp |Pune |20000 |
+-----+-------------+-----+------+

+-----+-----------+-----+------+
|empid|name       |place|salary|
+-----+-----------+-----+------+
|300  |New-Emp    |Agra |15000 |
|200  |Updated-Emp|Pune |25000 |
+-----+-----------+-----+------+
'''

# to identify new records : do left_anti join
new_df = source_df.join(target_df, 'empid', 'left_anti')
new_df.show()

'''
+-----+-------+-----+------+
|empid|   name|place|salary|
+-----+-------+-----+------+
|  300|New-Emp| Agra| 15000|
+-----+-------+-----+------+
'''



cond = [source_df.empid == target_df.empid]
base_df = target_df.join(source_df, cond, 'left')

# unchanged-records
unchanged_df = base_df.filter(source_df.empid.isNull()).select(target_df['*'])
unchanged_df.show()

'''
+-----+-------------+-----+------+
|empid|         name|place|salary|
+-----+-------------+-----+------+
|  100|Unchanged-Emp|Delhi| 10000|
+-----+-------------+-----+------+
'''

#updated records
updated_df = base_df.filter(source_df.empid.isNotNull()).select(source_df['*'])
updated_df.show()

'''
+-----+-----------+-----+------+
|empid|       name|place|salary|
+-----+-----------+-----+------+
|  200|Updated-Emp| Pune| 25000|
+-----+-----------+-----+------+

'''

# union all records
new_df.union(unchanged_df).union(updated_df).show()
+-----+-------------+-----+------+
|empid|         name|place|salary|
+-----+-------------+-----+------+
|  300|      New-Emp| Agra| 15000|
|  100|Unchanged-Emp|Delhi| 10000|
|  200|  Updated-Emp| Pune| 25000|
+-----+-------------+-----+------+
