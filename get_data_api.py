import requests
import json

# baca file json
def read_json_file():
    with open("api_data.json", "r") as file:
        data = json.load(file)

    token = None

    valid_data = []

    for row in data:
        # ambil data dari json
        application = row["application"]
        service_name = row["service_name"]
        service_type = row["service_type"]

        # ambil data dari api
        url = (
            f"https://gitlab.com/api/v4/projects/divistant.com%2Fdemo%2Finternal-demo%2Fapplications%2F{application}%2F{service_type}%2F{service_name}/languages"
        )
        print(f"URL: {url}")
        response = requests.get(url, headers={"PRIVATE-TOKEN": token})
        if response.status_code == 200:
            data = response.json()
            print(f"Data for {application}/{service_name}/{service_type}: {data}")
            if data != {}:
                valid_data.append(row)
            
        else:
            print(f"Failed to retrieve data for {application}/{service_name}/{service_type}. Status code: {response.status_code}")

    # simpan data ke file json
    with open("api_data_valid.json", "w") as file:
        json.dump(valid_data, file, indent=4)


def read_sql(uri, query_stmt):
    from sqlalchemy import create_engine

    # Create a database connection
    with create_engine(uri).connect() as connection:
        # Execute a query to fetch data
        result = connection.execute(query_stmt)
        data = result.fetchall()
    # convert to list of dict
    data = [dict(row) for row in data]
    return data

def main():
    # read sql
    uri = None
    query_stmt = "SELECT * FROM public.cicd_application limit 5"
    data = read_sql(uri, query_stmt)
    print(data)

if __name__ == "__main__":
    read_json_file()
