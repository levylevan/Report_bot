import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import pandas as pd
import pandahouse as ph
import seaborn as sns
import io
import telegram

from airflow.decorators import dag, task
from datetime import date, datetime, timedelta

token = '5584591914:AAFG7QMQHG2bUrduS_VS1dpZvEltp_tJXQc'
bot = telegram.Bot(token=token)
chat_id = '-817148946'

connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'user': 'student',
    'password': 'dpo_python_2020',
    'database': 'simulator_20221020'
}

yesterday = date.today() - timedelta(days=1)

default_args = {
    'owner': 'i-sidelnik',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 16)
}

schedule_interval = '0 11 * * *'

def set_options(title=None):
    plt.title(title, fontsize=15)
    plt.xlabel("")
    plt.ylabel("")
    plt.legend(fontsize=10)
    plt.xticks(rotation=10)
    plt.yticks(fontsize=10)
    
def add_data_labels(x_valyes, y_valyes, color='black', fontsize=10, format_string=None):
    for x, y in zip(x_valyes, y_valyes):
        s = y
        if format_string is not None:
            if format_string[-1] == 'K':
                s = format_string.format(y / 1000)
            else:
                s = format_string.format(y)
        plt.text(x=x,
                 y=y + y * 0.1,
                 s=s,
                 color=color, fontsize=fontsize)

def send_report(chat=None):
    chat_id = chat or 51388127

    #данные за предыдущие 7 дней
    query = '''
    select toDate(time) as date, 
     countIf(user_id, action == 'like') as likes,
     countIf(user_id, action == 'view') as views,
     uniq(user_id) as dau,
     countIf(user_id, action == 'like')/countIf(user_id, action == 'view') as ctr
    from simulator_20221020.feed_actions 
    where toDate(time) between today() - 7 and today() - 1
    group by date
    '''
    df = ph.read_clickhouse(query, connection=connection)

    #метрики за вчера
    yesterday_metrics = df[df.date == yesterday.strftime("%Y-%m-%d")]
    likes_yesterday = yesterday_metrics.likes.item()
    views_yesterday = yesterday_metrics.views.item()
    dau_yesterday = yesterday_metrics.dau.item()
    ctr_yesterday = yesterday_metrics.ctr.item()
    msg = f'*Отчет по ленте новостей.\nМетрики за вчера ({yesterday}):*\n' \
          f'DAU: `{dau_yesterday}`\n' \
          f'Просмотры: `{views_yesterday}`\n' \
          f'Лайки: `{likes_yesterday}`\n' \
          f'CTR: `{ctr_yesterday:.2f}`\n' \

    sns.set(rc={'figure.figsize': (12, 15)})
    sns.set_style('whitegrid')

    #график DAU
    color_dau = mcolors.CSS4_COLORS['darkviolet']
    plt.subplot(3, 1, 1)
    ax = sns.lineplot(data=df, x='date', y='dau', color=color_dau, label='DAU', marker='o')
    plt.ylim(bottom=0, top=df['dau'].max() * 1.3)
    y_labels = ['{:,.0f}'.format(y) + 'K' for y in ax.get_yticks() / 1000]
    y_ticks = [y for y in ax.get_yticks()]
    ax.set_yticks(y_ticks)
    ax.set_yticklabels(y_labels)
    add_data_labels(df['date'], df['dau'], color_dau, 12)
    set_options("DAU")

    #график лайков и просмотров
    color_likes = 'darkcyan'
    color_views = 'darkgreen'
    plt.subplot(3, 1, 2)
    ax = sns.lineplot(data=df, x='date', y='likes', color=color_likes, label='Лайки', marker='o')
    sns.lineplot(data=df, x='date', y='views', color=color_views, label='Просмотры', marker='o')
    plt.ylim(bottom=0, top=max(df['views'].max(), df['likes'].max()) * 1.2)
    y_labels = ['{:,.0f}'.format(y) + 'K' for y in ax.get_yticks() / 1000]
    y_ticks = [y for y in ax.get_yticks()]
    ax.set_yticks(y_ticks)
    ax.set_yticklabels(y_labels)
    add_data_labels(df['date'], df['views'], color_views, 12, '{:.0f}')
    add_data_labels(df['date'], df['likes'], color_likes, 12, '{:.0f}')
    set_options("Лайки и просмотры")

    #график CTR
    color_ctr = mcolors.CSS4_COLORS['darkorange']
    plt.subplot(3, 1, 3)

    ax = sns.lineplot(data=df, x='date', y='ctr', color=color_ctr, label='CTR', marker='o')
    plt.ylim(bottom=0, top=df['ctr'].max() * 1.3)
    y_labels = ['{:,.2f}'.format(y) for y in ax.get_yticks()]
    y_ticks = [y for y in ax.get_yticks()]
    ax.set_yticks(y_ticks)
    ax.set_yticklabels(y_labels)
    add_data_labels(df['date'], df['ctr'], color_ctr, 12, '{:.4f}')
    set_options("CTR")
    plt.tight_layout(pad=1.5)
    plot_object = io.BytesIO()
    plt.savefig(plot_object, dpi=300)
    plot_object.name = f'{yesterday}_weekdata.png'
    plot_object.seek(0)
    plt.close()

    bot.sendMessage(chat_id=chat_id, text=msg, parse_mode=telegram.ParseMode.MARKDOWN)
    bot.sendPhoto(chat_id=chat_id, photo=plot_object,
                  parse_mode=telegram.ParseMode.MARKDOWN)
    
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def sidelnik_report_bot():
    @task()
    def send_report_feed():
        chat_id = -817148946
        send_report(chat_id)

    send_report_feed()


sidelnik_report_bot = sidelnik_report_bot()