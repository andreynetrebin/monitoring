from dash import Dash, dcc, html, Input, Output, callback
import plotly.express as px
import pandas as pd
from sqlite3 import connect
import plotly.graph_objects as go
import dash_bootstrap_components as dbc

app = Dash(external_stylesheets=[dbc.themes.CYBORG], suppress_callback_exceptions=True)

app.css.config.serve_locally = False
app.scripts.config.serve_locally = True

conn = connect('metrics.db', check_same_thread=False)

options_insurer_report_def_keys = [
    {"label": "ЕФС-1", "value": "InsurerReportProcessingEFS"},
    {"label": "СЗВ-ТД", "value": "InsurerReportProcessingSZVTD"},
    {"label": "ИС", "value": "InsurerReportProcessingIS"},
    {"label": "СЗВ-М", "value": "InsurerReportProcessingSZVM"},
    {"label": "СЗВ-ТД ЕФС", "value": "InsurerReportProcessingEFSSZVTD"},
    {"label": "СЗВ-Бюдж", "value": "InsurerReportProcessingEFSBUDZ"},
    {"label": "СЗВ-СТАЖ ЕФС", "value": "InsurerReportProcessingSMALLIS"},
    {"label": "СЗИ-ФСС", "value": "InsurerReportProcessingSZIFSS"},
    {"label": "ДСВ-3", "value": "InsurerReportProcessingEFSDSV3"},
    {"label": "ОДВ-1", "value": "InsurerReportProcessingODV1EFS"},
]

options_person_application_def_keys = [

    {"label": "АДВ-1/2/3", "value": "SPU_Application_PersonADV_Processing"},
    {"label": "УСПН Оффлайн", "value": "ApplicationProccessingUSPNNoWait"},
    {"label": "АСВ", "value": "ApplicationProccessingASV"},
    {"label": "КСП", "value": "ApplicationProccessingKSP"},
    {"label": "МСК", "value": "ApplicationProccessingMSK"},
    {"label": "НВПиСВ", "value": "ApplicationProccessingNVPSV"},
    {"label": "НВПиСВ Оффлайн", "value": "ApplicationProccessingNVPSVNoWait"},
    {"label": "АДВ-СМ", "value": "ApplicationProccessingSPU"},

]

options_tsk_types = [

    {"label": "СЗИ-СПУ", "value": "TASK_SZI_SPU"},
    {"label": "АДВ-3", "value": "TASK_ADV-3_APPLICATION"},
    {"label": "АДИ-СМ", "value": "TASK_ADI_SM"},
    {"label": "ЕЦП-ВС1", "value": "TASK_ECP_VS1_FORM"},
]


def get_figure_range_slider(title, x_value, y_value):
    fig = go.Figure()

    fig.add_trace(
        go.Scatter(x=x_value, y=y_value)
    )

    # Set title
    fig.update_layout(
        title_text=title
    )

    # Add range slider
    fig.update_layout(
        xaxis=dict(
            rangeselector=dict(
                buttons=list([
                    dict(count=1,
                         label="1m",
                         step="month",
                         stepmode="backward"),
                    dict(count=7,
                         label="7d",
                         step="day",
                         stepmode="backward"),
                    dict(count=24,
                         label="24h",
                         step="hour",
                         stepmode="backward"),
                    dict(count=3,
                         label="3h",
                         step="hour",
                         stepmode="backward"),
                    dict(count=1,
                         label="1h",
                         step="hour",
                         stepmode="backward"),
                    dict(step="all")
                ])
            ),
            rangeslider=dict(
                visible=True
            ),
            type="date"
        )
    )
    fig.update_layout(template='plotly_dark',
                      xaxis_rangeselector_font_color='black',
                      xaxis_rangeselector_activecolor='red',
                      xaxis_rangeselector_bgcolor='green',
                      width=920, height=400
                      )
    return fig


app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])

index_page = html.Div([
    html.H2("Мониторинг"),
    html.A("Cервис страхователей (InsurerReport)", href='/mon_insurer_report', target='_blank'),
    html.Br(),
    html.A("Cервис заявлений (PersonApplication)", href='/mon_person_app', target='_blank'),
    html.Br(),
    html.A("СПУ фейлы/таски", href='/mon_spu_fails_tasks', target='_blank'),
])

mon_insurer_report_layout = html.Div([
    html.H3('Сервис страхователей (InsurerReport)'),
    html.Div([
        html.Div([
            dcc.RadioItems(
                options=options_insurer_report_def_keys,
                value='InsurerReportProcessingEFS',
                id='insurers',
                inline=True
            ),
        ]),
        html.Br(),

        html.Div([
            dcc.Graph(
                id="insurers_incidents",

            ),
        ], style={'width': '48%', 'display': 'inline-block'}),
        html.Div([
            dcc.Graph(
                id="insurers_instances"
            )
        ], style={'width': '48%', 'display': 'inline-block'}),
        html.Div([
            dcc.Graph(
                id="insurers_waiting_asv"
            )
        ], style={'width': '48%', 'display': 'inline-block'}),
        html.Div([
            dcc.Graph(
                id="insurers_waiting_spu"
            )
        ], style={'width': '48%', 'display': 'inline-block'}),
    ]),
])

mon_person_app_layout = html.Div([
    html.H3('Cервис заявлений (PersonApplication)'),
    html.Div([
        html.Div([
            dcc.RadioItems(
                options=options_person_application_def_keys,
                value='SPU_Application_PersonADV_Processing',
                id='persons',
                inline=True
            ),
        ]),
        html.Br(),
        html.Div([
            dcc.Graph(
                id="persons_incidents",
            ),
        ], style={'width': '48%', 'display': 'inline-block'}),
        html.Div([
            dcc.Graph(
                id="persons_instances"
            )
        ], style={'width': '48%', 'display': 'inline-block'}),
        html.Div([
            dcc.Graph(
                id="persons_reg"
            )
        ], style={'width': '48%', 'display': 'inline-block'}),
        html.Div([
            dcc.Graph(
                id="persons_decision"
            )
        ], style={'width': '48%', 'display': 'inline-block'}),
    ]),
])

mon_spu_fails_tasks_layout = html.Div([
    html.H3('СПУ фейлы/таски'),
    html.Div([
        html.Div([
            dcc.RadioItems(
                options=options_tsk_types,
                value='TASK_SZI_SPU',
                id='spu_fails_tasks',
                inline=True
            ),
        ]),
        html.Br(),
        html.Div([
            dcc.Graph(
                id="spu_fails",
            ),
        ], style={'width': '48%', 'display': 'inline-block'}),
        html.Div([
            dcc.Graph(
                id="spu_tasks"
            )
        ], style={'width': '48%', 'display': 'inline-block'}),
    ]),
])


@app.callback(Output('page-content', 'children'), Input('url', 'pathname'))
def display_page(pathname):
    if pathname == '/mon_insurer_report':
        return mon_insurer_report_layout
    elif pathname == '/mon_person_app':
        return mon_person_app_layout
    elif pathname == '/mon_spu_fails_tasks':
        return mon_spu_fails_tasks_layout
    else:
        return index_page


@app.callback(Output("insurers_incidents", "figure"), Input("insurers", "value"))
def render(insurers):
    df = pd.read_sql("""
        SELECT
            key_proc_def
            ,dt
            ,sum(cnt) cnt
        FROM
            camunda_incidents 
        GROUP BY
            key_proc_def
            ,dt
            """, conn)

    mask = df["key_proc_def"] == insurers
    x_value = df[mask].dt
    y_value = df[mask].cnt
    title = "Инциденты"

    fig = get_figure_range_slider(title, x_value, y_value)
    return fig


@app.callback(Output("insurers_instances", "figure"), Input("insurers", "value"))
def render(insurers):
    df = pd.read_sql("""
        SELECT
            key_proc_def
            ,dt
            ,sum(cnt) cnt
        FROM
            camunda_instances
        GROUP BY
            key_proc_def
            ,dt     
    """, conn)

    mask = df["key_proc_def"] == insurers
    x_value = df[mask].dt
    y_value = df[mask].cnt
    title = "Инстансы"

    fig = get_figure_range_slider(title, x_value, y_value)
    return fig


@app.callback(Output("insurers_waiting_asv", "figure"), Input("insurers", "value"))
def render(insurers):
    df = pd.read_sql("""
        SELECT
            key_proc_def
            ,dt
            ,cnt
        FROM
            camunda_instances 
        WHERE
            activity_id IN ('VerifyReportResponse_Evnt', 'verifyReportResponse_Event')
    """, conn)

    mask = df["key_proc_def"] == insurers
    x_value = df[mask].dt
    y_value = df[mask].cnt
    title = "В ожидании ответов от АСВ"

    fig = get_figure_range_slider(title, x_value, y_value)
    return fig


@app.callback(Output("insurers_waiting_spu", "figure"), Input("insurers", "value"))
def render(insurers):
    df = pd.read_sql("""
        SELECT
            key_proc_def
            ,dt
            ,cnt
        FROM
            camunda_instances 
        WHERE
            activity_id IN ('processReportResponse_Event', 'Event_1x1lljy')
    """, conn)

    mask = df["key_proc_def"] == insurers
    x_value = df[mask].dt
    y_value = df[mask].cnt
    title = "В ожидании ответов от СПУ"

    fig = get_figure_range_slider(title, x_value, y_value)
    return fig


#
# -----------------------------------------------------------
# 


@app.callback(Output("persons_incidents", "figure"), Input("persons", "value"))
def render(persons):
    df = pd.read_sql("""
        SELECT
            key_proc_def
            ,dt
            ,sum(cnt) cnt
        FROM
            camunda_incidents 
        GROUP BY
            key_proc_def
            ,dt
            """, conn)

    mask = df["key_proc_def"] == persons
    x_value = df[mask].dt
    y_value = df[mask].cnt
    title = "Инциденты"

    fig = get_figure_range_slider(title, x_value, y_value)
    return fig


@app.callback(Output("persons_instances", "figure"), Input("persons", "value"))
def render(persons):
    df = pd.read_sql("""
        SELECT
            key_proc_def
            ,dt
            ,sum(cnt) cnt
        FROM
            camunda_instances
        GROUP BY
            key_proc_def
            ,dt     
    """, conn)

    mask = df["key_proc_def"] == persons
    x_value = df[mask].dt
    y_value = df[mask].cnt
    title = "Инстансы"

    fig = get_figure_range_slider(title, x_value, y_value)
    return fig


# appUDSRegistration_Task appDecisionRecive_Event
@app.callback(Output("persons_reg", "figure"), Input("persons", "value"))
def render(persons):
    df = pd.read_sql("""
        SELECT
            key_proc_def
            ,dt
            ,cnt
        FROM
            camunda_instances 
        WHERE
            activity_id IN ('appUDSRegistration_Task')
    """, conn)

    mask = df["key_proc_def"] == persons
    x_value = df[mask].dt
    y_value = df[mask].cnt
    title = "Регистрация заявления в ЕХД"

    fig = get_figure_range_slider(title, x_value, y_value)
    return fig


@app.callback(Output("persons_decision", "figure"), Input("persons", "value"))
def render(persons):
    df = pd.read_sql("""
        SELECT
            key_proc_def
            ,dt
            ,cnt
        FROM
            camunda_instances 
        WHERE
            activity_id IN ('appDecisionRecive_Event')
    """, conn)

    mask = df["key_proc_def"] == persons
    x_value = df[mask].dt
    y_value = df[mask].cnt
    title = "Обработка заявления в ФП"

    fig = get_figure_range_slider(title, x_value, y_value)
    return fig


#
# --------------------------------------------------------------------
#


@app.callback(Output("spu_fails", "figure"), Input("spu_fails_tasks", "value"))
def render(spu_fails_tasks):
    df = pd.read_sql("""
        SELECT
            tsk_type
            ,dt
            ,cnt
        FROM
            spu_fails 
            """, conn)

    mask = df["tsk_type"] == spu_fails_tasks
    x_value = df[mask].dt
    y_value = df[mask].cnt
    title = "Фейлы"

    fig = get_figure_range_slider(title, x_value, y_value)
    return fig


@app.callback(Output("spu_tasks", "figure"), Input("spu_fails_tasks", "value"))
def render(spu_fails_tasks):
    df = pd.read_sql("""
        SELECT
            tsk_type
            ,dt
            ,cnt
        FROM
            spu_tasks 
            """, conn)

    mask = df["tsk_type"] == spu_fails_tasks
    x_value = df[mask].dt
    y_value = df[mask].cnt
    title = "Таски"

    fig = get_figure_range_slider(title, x_value, y_value)
    return fig


if __name__ == "__main__":
    app.run_server()
