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

''' <script type="text/javascript" charset="utf-8" async src="https:
    //api-maps.yandex.ru/services/constructor/1.0/js/?um=constructor%3A3a6741054b0715f2530d2808570c
    715a7ea94398c0dce71d6f2b4ceeef67faf9&amp;width=537&amp;height=438&amp;lang=ru_RU&amp;scroll=true"></script> 
    
    {
        'async src': 'https://api-maps.yandex.ru/services/constructor/1.0/js/?um=constructor%3A3a6741054b0715f25\
        30d2808570c715a7ea94398c0dce71d6f2b4ceeef67faf9&amp;width=537&amp;height=438&amp;lang=ru_RU&amp;scroll=true'
    }
   
'''
external_scripts = [
    {"src": "https://api-maps.yandex.ru/3.0/?apikey=2ea5f3ce-e677-4bb0-8834-12ee29730cb8&lang=ru_RU"}
]

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], external_scripts=external_scripts)

tab1_content = dbc.Tab(
    [
        dbc.Row(
            [
                dbc.Col(
                    [
                        html.Div("Количество комнат: "),
                        dcc.Checklist(
                            id='menu',
                            options=['1', '2', '3', '4', '5 и более'],
                            value=['1', '2', '3', '4', '5 и более'],
                        )
                    ]
                )
            ],
            style={'margin-bottom': 40}
        ),
        dbc.Row(
            [
                dbc.Col(
                    [
                        html.Div("Graph1"),
                        dcc.Graph(id='gr1')
                    ],
                    width={'size': 6}
                ),
                dbc.Col(
                    [
                        html.Div("Graph2"),
                        dcc.Graph(id='gr2')
                    ],
                    width={'size': 6}
                )
            ],
            style={'margin-bottom': 40}
        )
    ]
)

tab2_content = (
    html.Script("https://api-maps.yandex.ru/3.0/?apikey=2ea5f3ce-e677-4bb0-8834-12ee29730cb8&lang=ru_RU"),
    dbc.Button(id="button"),
    html.Div(id="testDiv")
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
        query = f"SELECT last_price, distance_to_center \
                    FROM table \
                    WHERE number_of_rooms IN ({prop}) OR number_of_rooms >= 5"
    elif prop:
        query = f"SELECT last_price, distance_to_center \
                    FROM table \
                    WHERE number_of_rooms IN ({prop})"
    elif '5 и более' in lst:
        query = f"SELECT last_price, distance_to_center \
                    FROM table \
                    WHERE number_of_rooms >= 5"
    else:
        query = f"SELECT last_price, distance_to_center \
                    FROM table \
                    WHERE number_of_rooms = -1"  # просто для пустой таблицы

    data = spark.sql(query).toPandas()

    fig = px.scatter(data, x='distance_to_center', y='last_price')
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
        query = f"SELECT district, count(*) num FROM realtby_data_table\
                            WHERE number_of_rooms = -1"  # просто для пустой таблицы

    data = spark.sql(query).toPandas()
    fig = px.histogram(data, x="district", y="num")
    return fig


app.clientside_callback(
    """
    ymaps3.ready.then(init);
    function init() {
        const map = new ymaps3.YMap(document.getElementById('testDiv'), {
          location: {
            center: [37.64, 55.76],
            zoom: 7
          }
        });
    }
    """,
    Output('testDiv', 'children'),
    Input('button', 'n_clicks')
)

if __name__ == '__main__':
    app.run_server(debug=True)
