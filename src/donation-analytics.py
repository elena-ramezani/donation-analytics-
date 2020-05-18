import findspark
findspark.init("/usr/local/spark", edit_rc=True)
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
import pdb
import datetime
import os
import math
import sys

sc = SparkContext("local[*]", "Elnaz App")
input_file = sys.argv[1]
output_file = sys.argv[3]
percentile_file = sys.argv[2]

with open(percentile_file, "r") as f:
    percentile = float(f.read())


def calc_percentile(element_list, percentile):
    index = math.ceil((percentile / 100) * len(element_list))
    return element_list[index - 1]


def write_file(line):
    with open(output_file, "a") as f:
        f.write(line + "\n")


# cheching the valid zip code
def get_zip(data):
    zip_code = data[:5]
    # Ignoring ZIP length less than 5
    if len(zip_code) < 5:
        return False
    return zip_code


def get_date(data):
    try:
        # Checking proper date format. ALso works for empty date string
        if (datetime.datetime.strptime(data, "%m%d%Y")):
            tr_date = data[-4:]
    except:
        print("Date Exception")
        return False
    return tr_date


if __name__ == '__main__':
    if (os.path.exists(output_file)):
        os.remove(output_file)

    rdd_data = sc.textFile(input_file)
    data = rdd_data.map(lambda x: x.split('|'))

    ## select needed features
    # CMTE_ID, Name, ZIP_CODE,TRANSACTION_DT,TRANSACTION_AMT, OTHER_ID= x[15]
    selected_data = data.map(lambda x: (x[0], x[7], get_zip(x[10]), get_date(x[13]), x[14], x[15]))

    # Checking valid zip code and time
    filter_data = selected_data.filter(lambda x: x[2] and x[3] and not x[5])

    # CMTE_ID, ZIP_CODE,YEAR ,NAME, TRANSACTION_AMT
    selected_data = filter_data.map(lambda x: (x[0], x[2], x[3], x[1], x[4]))

    spark = SparkSession(sc)

    # convert rdd to data farme
    spark_df = selected_data.toDF()

    # creating header
    spark_df = spark_df.withColumnRenamed("_1", "CMTE_ID").withColumnRenamed("_2", "ZIPCODE").withColumnRenamed("_3",
                                                                                                                "YEAR").withColumnRenamed(
        "_4", "NAME").withColumnRenamed("_5", "TRANSACTION_AMT")
    spark_df.show()

    # grouping by zipcode and name of the donors
    group_data = spark_df.groupby("ZIPCODE", "NAME")
    repeated_count = group_data.agg({"NAME": 'count'})

    # filtering by the repeated donors who has donated more than once
    repeated_donor = repeated_count.filter("count(NAME) >  1")
    join_spark_df = spark_df.join(repeated_donor, repeated_donor.NAME == spark_df.NAME, how='left_semi')

    # taking only list in 2018
    contribution_2018 = join_spark_df.filter("YEAR = 2018")
    contribution_2018.show()

    ## print results
    df = contribution_2018.toPandas()
    repeated_donor = []

    for i in range(len(df)):
        repeated_donor.append(df['NAME'].loc[i])
        select_name = df[df['NAME'].isin(repeated_donor)]
        select_CMTE = select_name[select_name['CMTE_ID'] == select_name['CMTE_ID'].loc[i]]
        total_amount_list = select_CMTE['TRANSACTION_AMT'].astype(int)
        total_amount = total_amount_list.sum()
        # pdb.set_trace()
        total_per = calc_percentile(total_amount_list, percentile)
        number_of_repeat_donor = len(total_amount_list)

        out = str(df['CMTE_ID'].loc[i]) + '|' + str(df['ZIPCODE'].loc[i]) + '|' + str(df['YEAR'].loc[i]) + '|' + str(
            total_per) + '|' + str(total_amount) + '|' + str(number_of_repeat_donor)

        write_file(out)
