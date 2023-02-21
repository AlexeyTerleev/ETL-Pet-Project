import dash
from dash import html, dcc
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc

import plotly.express as px

from pyspark.sql import SparkSession

spark = SparkSession.builder.config("spark.jars", "/usr/local/postgresql-42.5.1.jar") \
    .master("local").appName("PySpark_Postgres_test").getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/pet_project_db") \
    .option("dbtable", "realtby_data_table") \
    .option("user", "postgres") \
    .option("password", "changeme") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df.createOrReplaceTempView("table")


app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

tab1_content = dbc.Tab(
    dbc.Row(
        [
            dbc.Col(
                [
                    dbc.Row(
                        [
                            dcc.Graph(id='gr1')
                        ],
                        style={'border-bottom': 'solid', 'border-width': '1px', 'border-color': '#5d60fc'}
                    ),
                    dbc.Row(
                        [
                            dcc.Graph(id='gr2')
                        ]
                    )
                ],
                width={'size': 7}
            ),
            dbc.Col(
                width={'size': 1}
            ),
            dbc.Col(
                [

                    html.Div(
                        [
                            html.Div("Количество комнат: "),
                            dcc.Checklist(
                                id='menu',
                                options=['1', '2', '3', '4', '5 и более'],
                                value=['1', '2', '3', '4', '5 и более'],
                                labelStyle={'display': 'block'},
                                style={"overflow": "auto"},
                            )
                        ],
                        style={'padding': 20, 'border': 'solid', 'border-width': '1px', 'border-color': '#5d60fc'}
                    ),
                ],
                style={'align-self': 'center'},
                width={'size': 3}
            ),
            dbc.Col(
                width={'size': 1}
            ),
        ],
        style={'margin-bottom': 40}
    )
)

tab2_content = (

)

app.layout = html.Div([
    dbc.Row(
        [
            dbc.Col(
                html.H2("header")
            )
        ],
        style={'margin-bottom': 40}
    ),
    dbc.Tabs(
        [
            dbc.Tab(tab1_content, label="first page"),
            dbc.Tab(tab2_content, label="second page")
        ]
    )
], style={'margin-left': 80, 'margin-right': 80})


@app.callback(
    Output(component_id='gr1', component_property='figure'),
    Input(component_id='menu', component_property='value')
)
def filter_by_num_of_rooms(lst):
    prop = ', '.join(x for x in lst if x != '5 и более')
    if prop and '5 и более' in lst:
        query = f"SELECT last_price, distance_to_center, number_of_rooms rooms\
                    FROM table \
                    WHERE number_of_rooms IN ({prop}) OR number_of_rooms >= 5"
    elif prop:
        query = f"SELECT last_price, distance_to_center, number_of_rooms rooms  \
                    FROM table \
                    WHERE number_of_rooms IN ({prop})"
    elif '5 и более' in lst:
        query = f"SELECT last_price, distance_to_center, number_of_rooms rooms \
                    FROM table \
                    WHERE number_of_rooms >= 5"
    else:
        query = f"SELECT last_price, distance_to_center, number_of_rooms rooms \
                    FROM table \
                    WHERE number_of_rooms = -1"  # просто для пустой таблицы

    data = spark.sql(query).toPandas()
    data["rooms"] = data["rooms"].astype(str)

    fig = px.scatter(data, x='distance_to_center', y='last_price', color='rooms',
                     title='Зависимость стоимости от расположения')
    fig.update_layout(
        xaxis_title='Расстояние до центра (км)',
        yaxis_title='Цена ($)'
    )
    return fig


@app.callback(
    Output(component_id='gr2', component_property='figure'),
    Input(component_id='menu', component_property='value')
)
def filter_by_num_of_rooms(lst):
    prop = ', '.join(x for x in lst if x != '5 и более')
    if prop and '5 и более' in lst:
        query = f"SELECT district, count(*) num FROM table\
                    WHERE number_of_rooms IN ({prop}) OR number_of_rooms >= 5 \
                    GROUP BY district"
    elif prop:
        query = f"SELECT district, count(*) num FROM table\
                    WHERE number_of_rooms IN ({prop}) \
                    GROUP BY district"
    elif '5 и более' in lst:
        query = f"SELECT district, count(*) num FROM table\
                    WHERE number_of_rooms >= 5 \
                    GROUP BY district"
    else:
        query = f"SELECT district, count(*) num FROM table\
                    WHERE number_of_rooms = -1 \
                    GROUP BY district"  # просто для пустой таблицы

    data = spark.sql(query).toPandas()

    fig = px.histogram(data, x="district", y="num", title='Отражение количества продоваемых квартир в разных районах')
    fig.update_layout(
        xaxis_title='',
        yaxis_title='Количество продоваемых квартир'
    )
    return fig


if __name__ == '__main__':
    app.run_server(debug=True)
