
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

cond = [source_df.empid == target_df.empid]
base_df = target_df.join(source_df, cond, 'full')

base_df.show()

'''
+-----+-------------+-----+------+-----+-----------+-----+------+
|empid|         name|place|salary|empid|       name|place|salary|
+-----+-------------+-----+------+-----+-----------+-----+------+
|  100|Unchanged-Emp|Delhi| 10000| null|       null| null|  null|
|  200| Obsolete-Emp| Pune| 20000|  200|Updated-Emp| Pune| 25000|
| null|         null| null|  null|  300|    New-Emp| Agra| 15000|
+-----+-------------+-----+------+-----+-----------+-----+------+
'''

#unchanged-records
unchanged_df = base_df.filter(source_df.empid.isNull()).select(target_df['*'])
unchanged_df.show()
'''
+-----+-------------+-----+------+
|empid|         name|place|salary|
+-----+-------------+-----+------+
|  100|Unchanged-Emp|Delhi| 10000|
+-----+-------------+-----+------+
'''

#updated and new records
new_updated_df = base_df.filter(source_df.empid.isNotNull()).select(source_df['*'])
new_updated_df.show()
'''
+-----+-----------+-----+------+
|empid|       name|place|salary|
+-----+-----------+-----+------+
|  200|Updated-Emp| Pune| 25000|
|  300|    New-Emp| Agra| 15000|
+-----+-----------+-----+------+
'''

final_df = unchanged_df.union(new_updated_df)
final_df.show()
'''
+-----+-------------+-----+------+
|empid|         name|place|salary|
+-----+-------------+-----+------+
|  100|Unchanged-Emp|Delhi| 10000|
|  200|  Updated-Emp| Pune| 25000|
|  300|      New-Emp| Agra| 15000|
+-----+-------------+-----+------+
'''
