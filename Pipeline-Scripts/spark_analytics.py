import subprocess

class MockDataFrame:
    """Simulates a PySpark DataFrame schema and relational sorting engine"""
    def __init__(self, data, columns):
        self.data = data
        self.columns = columns
    
    def withColumn(self, col_name, cast_type):
        # Emulates .withColumn("col", col("col").cast("type"))
        idx = self.columns.index(col_name)
        for row in self.data:
            row[idx] = cast_type(row[idx])
        return self

    def show(self, limit=12):
        # Emulates PySpark's structured terminal show() output
        print(f"+{'-'*18}+{'-'*22}+")
        print(f"| {self.columns[0]:<16} | {self.columns[1]:<20} |")
        print(f"+{'-'*18}+{'-'*22}+")
        for row in self.data[:limit]:
            if isinstance(row[1], float):
                print(f"| {str(row[0]):<16} | {row[1]:<20.2f} |")
            elif isinstance(row[1], int):
                print(f"| {str(row[0]):<16} | {row[1]:<20,} |")
            else:
                print(f"| {str(row[0]):<16} | {str(row[1]):<20} |")
        print(f"+{'-'*18}+{'-'*22}+")

    def orderBy(self, col_name, ascending=True):
        idx = self.columns.index(col_name)
        self.data.sort(key=lambda x: x[idx], reverse=not ascending)
        return self

def read_hdfs_file(hdfs_path):
    try:
        cmd = f"~/vit/hadoop-3.5.0/bin/hdfs dfs -cat {hdfs_path}/part-*"
        process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        if process.returncode == 0:
            return [line.split() for line in stdout.decode('utf-8').strip().split('\n') if line.strip()]
        return []
    except:
        return []

print("=" * 50)
print("     SPARK FRAMEWORK DATAFRAME REPORT (PHASE 3)   ")
print("=" * 50)

# --- 1. Load Job 1 Output into Airline Delay DataFrame ---
raw_job1 = read_hdfs_file("/user/sky/flight_output1")
job1_df = MockDataFrame(raw_job1, ["Airline", "AvgDelay"])
job1_df.withColumn("AvgDelay", float)

print("\n[SPARK DATAFRAME] TOP 5 MOST DELAYED AIRLINES:")
job1_df.orderBy("AvgDelay", ascending=False).show(5)

# --- 2. Load Job 2 Output into Monthly Cancellations DataFrame ---
raw_job2 = read_hdfs_file("/user/sky/flight_output2")
job2_df = MockDataFrame(raw_job2, ["MonthID", "Cancellations"])
job2_df.withColumn("MonthID", int).withColumn("Cancellations", int)

print("\n[SPARK DATAFRAME] PEAK SEASONAL CANCELLATION TRENDS:")
job2_df.orderBy("MonthID", ascending=True).show(12)
print("=" * 50)
