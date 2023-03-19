# Query Statistics
A Streamlit app to help visualize and highlight certain query statistics, using the Snowflake Query Statistics.

## How to Install:
```
conda create -n snowflake-query-statistics python=3.8
conda activate snowflake-query-statistics
```

Then run one of the following:
```
pip install streamlit
pip install git+https://github.com/sfc-gh-brianhess/st_connection.git#egg=st_connection
pip install pyarrow~=10.0.1
```

or

```
pipenv install
pipenv shell
pipenv install streamlit
pipenv install git+https://github.com/sfc-gh-brianhess/st_connection.git#egg=st_connection
pipenv install pyarrow~=10.0.1
```

## How to Run:
```
streamlit run streamlit_app.py
```