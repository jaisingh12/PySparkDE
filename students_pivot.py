from pyspark.sql import functions as F

data = [ (10, 75,'english'), 
         (10, 80, 'hindi'), 
         (11, 65,'english'), 
         (11, 90, 'hindi'), 
         (11, 63, 'science'), 
         (12, 93, 'science'), 
         (12, 88, 'english') ]

schema = ['rno','marks','subject']
df = spark.createDataFrame(data, schema)
df.show(20,0)


###  OUTPUT ###
+---+-----+-------+
|rno|marks|subject|
+---+-----+-------+
|10 |75   |english|
|10 |80   |hindi  |
|11 |65   |english|
|11 |90   |hindi  |
|11 |63   |science|
|12 |93   |science|
|12 |88   |english|
+---+-----+-------+
###############


pivot_df = df.groupBy('rno').pivot('subject').agg(F.first("marks")).fillna(0)
pivot_df.show()

###  OUTPUT ###
+---+-------+-----+-------+
|rno|english|hindi|science|
+---+-------+-----+-------+
| 10|     75|   80|      0|
| 12|     88|    0|     93|
| 11|     65|   90|     63|
+---+-------+-----+-------+
###############

# Add a new column with the sum of marks for each 'rno'
pivot_df_with_sum = pivot_df.withColumn("total_marks", sum(F.col(subject) for subject in pivot_df.columns[1:]))
pivot_df_with_sum.show()

###  OUTPUT ###
+---+-------+-----+-------+---------+
|rno|english|hindi|science|marks_sum|
+---+-------+-----+-------+---------+
| 10|     75|   80|      0|      155|
| 12|     88|    0|     93|      181|
| 11|     65|   90|     63|      218|
+---+-------+-----+-------+---------+

###############
