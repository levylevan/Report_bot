import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import telegram
import pandahouse as ph
import io
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

sns.set(rc={'figure.figsize':(11.7,8.27)})
sns.set_context('talk')
sns.set_style("whitegrid")

BOT_TOKEN = '5584591914:AAFG7QMQHG2bUrduS_VS1dpZvEltp_tJXQc'

CHAT_ID = -715927362

bot = telegram.Bot(token=BOT_TOKEN)

METRICS_LIST = ['feed_users', 'views', 'likes', 'ctr', 'messages_users', 'messages_sent']

connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'database': 'simulator_20221020',
    'user': 'student',
    'password': 'dpo_python_2020'

}

default_args = {
    'owner': 'i-sidelnik',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 18),
}

schedule_interval = '15 * * * *'

metrics_query = """
                    SELECT
                        f.ts
                        , toDate(f.ts) "date"
                        , formatDateTime(f.ts, '%R') hm
                        , feed_users
                        , views
                        , likes
                        , ctr
                        , m.messages_users
                        , m.messages_sent
                    FROM (
                        SELECT
                            toStartOfFifteenMinutes(time) ts
                            , uniqExact(user_id) feed_users
                            , countIf(user_id, action='view') views
                            , countIf(user_id, action='like') likes
                            , countIf(user_id, action='like') / countIf(user_id, action='view') ctr
                        FROM
                            {db}.feed_actions
                        WHERE 
                            ts >=  today() - 1 and ts < toStartOfFifteenMinutes(now())
                        GROUP BY 
                            ts
                            ) f
                    LEFT JOIN (
                        SELECT
                            toStartOfFifteenMinutes(time) ts
                            , uniqExact(user_id) messages_users
                            , count(user_id) messages_sent
                        FROM
                            {db}.message_actions
                        WHERE 
                            ts >=  today() - 1 and ts < toStartOfFifteenMinutes(now())
                        GROUP BY 
                            ts
                            ) m ON m.ts = f.ts
                    ORDER BY
                        f.ts
                        """

def anomaly_check(df, metric, a = 2, n = 7):

    df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
    df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)

    df['iqr'] = df['q75'] - df['q25']

    df['low'] = df['q25'] - a * df['iqr']
    df['high'] = df['q75'] + a * df['iqr']

    df['low'] = df['low'].rolling(n, center = True, min_periods=1).mean()
    df['high'] = df['high'].rolling(n, center = True, min_periods=1).mean()

    if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['high'].iloc[-1]:
        alert = 1
    else:
        alert = 0

    return df, alert

def send_report(df, metric):

    diff = 1 - df[metric].iloc[-1] / df[metric].iloc[-2]

    if diff < 0:
        marker = '🟢'
        text = 'ПИК'
    elif diff > 0:
        marker = '🔴'
        text = 'ПРОВАЛ'
    else:
        marker = '🟡'

    msg = f"""
    {marker*5} *{text}*
*Аномальное значение метрики {metric}!*
Значение метрики: *{df[metric].iloc[-1]:.2f}*
Отклонение *{abs(diff):.0%}*
    """

    ax = sns.lineplot(data=df,
                        x=df['ts'],
                        y=metric,
                        label=metric)
    ax = sns.lineplot(data=df,
                        x=df['ts'],
                        y='high',
                        label='high')
    ax = sns.lineplot(data=df,
                        x=df['ts'],
                        y='low',
                        label='low')

    plt.ylim(0, None)
    plt.xlabel('time')
    plt.ylabel('value')

    plt.title(f'{metric} anomaly!')
    plt.xticks(rotation=-25, ha='left')

    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = f'{metric}.png'
    plt.close()

    bot.sendMessage(chat_id=CHAT_ID, text=msg, parse_mode = 'Markdown')
    bot.sendPhoto(chat_id=CHAT_ID, photo=plot_object)


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def Sidelnik_alerts():

    @task()
    def extract_data(query):
        return ph.read_clickhouse(query, connection=connection)

    @task()
    def transform_send(df, metrics_list = METRICS_LIST):

        for metric in metrics_list:
            slice = df[['ts', 'date', 'hm', metric]].copy()
            
            df_anomaly, alert = anomaly_check(slice, metric)

            if alert == 1:

                send_report(df_anomaly, metric)

    df = extract_data(metrics_query)

    transform_send(df)

Sidelnik_alerts = Sidelnik_alerts()
