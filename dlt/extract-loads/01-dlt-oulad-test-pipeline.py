import os
import dlt
import pandas as pd

# Data directory inside container (matches compose.yaml mount)
DATA_DIR = "/app/data/oulad_test1"

# Filenames only (not full paths)
FILES = {
    "courses": "courses.csv",
    "assessments": "assessments.csv",
    "student_info": "studentInfo.csv",
    "student_registration": "studentRegistration.csv",
    "student_assessment": "studentAssessment.csv",
    "vle": "vle.csv",
}

def load_csv(file_name: str):
    """Helper to load CSV into pandas DataFrame and yield rows as dict."""
    path = os.path.join(DATA_DIR, file_name)
    df = pd.read_csv(path)
    for row in df.to_dict(orient="records"):
        yield row

# Resources for each dataset
@dlt.resource(write_disposition="append", name="student_info")
def student_info():
    yield from load_csv(FILES["student_info"])

@dlt.resource(write_disposition="append", name="student_assessment")
def student_assessment():
    yield from load_csv(FILES["student_assessment"])

@dlt.resource(write_disposition="append", name="courses")
def courses():
    yield from load_csv(FILES["courses"])

@dlt.resource(write_disposition="append", name="vle")
def vle():
    yield from load_csv(FILES["vle"])

@dlt.resource(write_disposition="append", name="student_registration")
def student_registration():
    yield from load_csv(FILES["student_registration"])

@dlt.resource(write_disposition="append", name="assessments")
def assessments():
    yield from load_csv(FILES["assessments"])

def run():
    pipeline = dlt.pipeline(
        pipeline_name="test_g1_oulad_pipeline",   # <-- renamed to avoid confusion
        destination="clickhouse",
        dataset_name="oulad_test1",
        dev_mode=False   # set True if you want to overwrite on each run
    )

    print("Loading OULAD data into ClickHouse...")

    load_info = pipeline.run(student_info())
    print("student_info loaded:", load_info)

    load_info = pipeline.run(student_assessment())
    print("student_assessment loaded:", load_info)

    load_info = pipeline.run(courses())
    print("courses loaded:", load_info)

    load_info = pipeline.run(vle())
    print("vle loaded:", load_info)

    load_info = pipeline.run(student_registration())
    print("student_registration loaded:", load_info)

    load_info = pipeline.run(assessments())
    print("assessments loaded:", load_info)

if __name__ == "__main__":
    run()
