import requests
import pandas as pd
import numpy as np

import seaborn as sns
import matplotlib.pyplot as plt

from datetime import timedelta
from datetime import datetime
import json
from urllib.parse import urlencode

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

default_args = {
    'owner': 'a.sibiriakova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 15),
    'schedule_interval': '45 10 * * *'
}


CHAT_ID = 344844265
try:
    BOT_TOKEN = '5789264690:AAE8L0uRRF1iiGCh-XxSKBeiltKTnvXI9AE'
except:
    BOT_TOKEN = ''   
    
def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Dag {dag_id} - {date}. Данные успешно обновлены. Лови обновленные графики!'
    if BOT_TOKEN != '':
        params = {'chat_id': CHAT_ID, 'text': message}

        base_url = f'https://api.telegram.org/bot{BOT_TOKEN}/'
        url = base_url + 'sendMessage?' + urlencode(params)
        resp = requests.get(url)
        
        CR = {'photo': open('CR.png', 'rb')}
        ARPU = {'photo': open('ARPU_ARPAU_ARPPU.png', 'rb')}
        
        url = f'{base_url}sendPhoto?chat_id={CHAT_ID}'
        resp = requests.post(url, files=CR)
        resp = requests.post(url, files=ARPU)
    else:
        pass

def get_csv_from_yandex(link):    
    # используем api 
    base_url = 'https://cloud-api.yandex.net/v1/disk/public/resources/download?' 
        
    # получаем url 
    final_url = base_url + urlencode(dict(public_key=link)) 
    response = requests.get(final_url) 
    download_url = response.json()['href'] 
        
    return download_url

GROUPS_LINK = 'https://disk.yandex.ru/d/UhyYx41rTt3clQ'
ACTIVE_STUDS_LINK = 'https://disk.yandex.ru/d/Tbs44Bm6H_FwFQ'
CHECKS_LINK = 'https://disk.yandex.ru/d/pH1q-VqcxXjsVA'

GROUPS_ADD_LINK = 'https://disk.yandex.ru/d/5Kxrz02m3IBUwQ'
    
@dag(default_args=default_args, catchup=False)
def final_project_variant_2_a_sibiriakova_15_08():  
    @task(retries=3)
    def get_data(link, sep=''):
        if len(sep) > 0:
            df = pd.read_csv(get_csv_from_yandex(link),sep=sep)
        else:
            df = pd.read_csv(get_csv_from_yandex(link))
        return df

    # функция, которая будет автоматически подгружать информацию из дополнительного файла groups_add.csv (заголовки могут отличаться) 
    # и на основании дополнительных параметров пересчитывать метрики
    @task()
    def get_update_data(groups_df, active_studs_df, checks_df):
        # на вход функции подаем датафреймы с имеющимися данными

        # загружаем дополнительные данные из файла
        groups_add_df = pd.read_csv(get_csv_from_yandex(GROUPS_ADD_LINK)) 

        # объединяем основной файл с пользователями с дополнительным
        groups_all_df = groups_df.append(groups_add_df, ignore_index=False, verify_integrity=False, sort=None)

        # объединяем датафрейм с группами пользователей с датафреймами с активным пользователям и платящим пользователям
        # датафрейм checks_df объединяем с датафреймом active_studs_df, чтобы были платежи только активных пользователей
        data_all = groups_all_df \
            .merge(active_studs_df, how='left', left_on='id', right_on='student_id') \
            .merge(checks_df, how='left', left_on='student_id', right_on='student_id') \
            .rename(columns={'student_id_x':'active_stud', 'student_id_y':'paying_stud'})

        # группируем пользователей по группам и считаем число всех, активных и платящих, и сумму дохода в каждой группе
        # последней строчкой удаляем мультииндекс
        metrics_df = data_all \
            .groupby('grp', as_index=False) \
            .agg({'id':[('all_stud', 'count')], 
                  'student_id':[('active_stud', 'count')], 
                  'rev':[('paying_stud','count'),('revenue', 'sum')]}) \
            .droplevel(0, axis=1)

        # переименовываем первый столбец, так как при удалении индекса потерялось его название
        metrics_df = metrics_df.rename(columns={metrics_df.columns[0]: 'grp'})
    
        # считаем соотношение активных пользователей ко всем пользователям в группе
        CR_A = round(metrics_df[(metrics_df.grp == 'A')]['active_stud'] / \
                     metrics_df[(metrics_df.grp == 'A')]['all_stud'] * 100, 2)
        CR_B = round(metrics_df[(metrics_df.grp == 'B')]['active_stud'] / \
                     metrics_df[(metrics_df.grp == 'B')]['all_stud'] * 100, 2)
        metrics_df['CR_AU'] = pd.Series([CR_A.values[0], CR_B.values[0]], index=[0, 1])

        # считаем соотношение платящих пользователей ко всем пользователям в группе
        CR_A = round(metrics_df[(metrics_df.grp == 'A')]['paying_stud'] / \
                     metrics_df[(metrics_df.grp == 'A')]['all_stud'] * 100, 2)
        CR_B = round(metrics_df[(metrics_df.grp == 'B')]['paying_stud'] / \
                     metrics_df[(metrics_df.grp == 'B')]['all_stud'] * 100, 2)
        metrics_df['CR_PU'] = pd.Series([CR_A.values[0], CR_B.values[0]], index=[0, 1])

        # считаем соотношение платящих пользователей к активным
        CR_A = round(metrics_df[(metrics_df.grp == 'A')]['paying_stud'] / \
                     metrics_df[(metrics_df.grp == 'A')]['active_stud'] * 100, 2)
        CR_B = round(metrics_df[(metrics_df.grp == 'B')]['paying_stud'] / \
                     metrics_df[(metrics_df.grp == 'B')]['active_stud'] * 100, 2)
        metrics_df['CR_AU_to_PU'] = pd.Series([CR_A.values[0], CR_B.values[0]], index=[0, 1])

        # считаем отношение дохода к числу всех пользователей в группе
        ARPU_A = round(metrics_df[(metrics_df.grp == 'A')]['revenue'] / \
                        metrics_df[(metrics_df.grp == 'A')]['all_stud'], 2)
        ARPU_B = round(metrics_df[(metrics_df.grp == 'B')]['revenue'] / \
                        metrics_df[(metrics_df.grp == 'B')]['all_stud'], 2)
        metrics_df['ARPU'] = pd.Series([ARPU_A.values[0], ARPU_B.values[0]], index=[0, 1])

        # считаем отношение дохода к числу активных пользователей
        ARPAU_A = round(metrics_df[(metrics_df.grp == 'A')]['revenue'] / \
                        metrics_df[(metrics_df.grp == 'A')]['active_stud'], 2)
        ARPAU_B = round(metrics_df[(metrics_df.grp == 'B')]['revenue'] / \
                        metrics_df[(metrics_df.grp == 'B')]['active_stud'], 2)
        metrics_df['ARPAU'] = pd.Series([ARPAU_A.values[0], ARPAU_B.values[0]], index=[0, 1])

        # считаем отношение дохода к числу платящих пользователей
        ARPPU_A = round(metrics_df[(metrics_df.grp == 'A')]['revenue'] / \
                        metrics_df[(metrics_df.grp == 'A')]['paying_stud'], 2)
        ARPPU_B = round(metrics_df[(metrics_df.grp == 'B')]['revenue'] / \
                        metrics_df[(metrics_df.grp == 'B')]['paying_stud'], 2)
        metrics_df['ARPPU'] = pd.Series([ARPPU_A.values[0], ARPPU_B.values[0]], index=[0, 1])

        # возвращаем датафрейм с рассчитанными метриками
        return metrics_df  

    # функция, которая будет строить графики по получаемым метрикам и сохранять картинку в файл
    @task(on_success_callback=send_message)
    def get_grafics_of_metrics(metrics_df):
        sns.set(rc={'figure.figsize':(12,6)}, style="white")

        # строим график
        splot = sns.barplot(data=metrics_df[['CR_AU','CR_PU', 'CR_AU_to_PU', 'grp']]\
                            .melt(id_vars='grp',value_name='value', var_name='metrics'),
                            x='value', y='metrics', hue='grp')
        
        # подписываем бары на графике
        for g in splot.patches:    
            splot.annotate(format(g.get_width(), '.2f'),  
                (g.get_width(), g.get_y() + g.get_height() / 2.),                   
                ha = 'center', va = 'center',                   
                xytext = (18, 0),                   
                textcoords = 'offset points',
                color = 'grey',
                fontweight = 'bold',
                fontsize = 12)
            
        # убираем границы
        right_side = splot.spines["right"]
        right_side.set_visible(False)
        top_side = splot.spines["top"]
        top_side.set_visible(False)
        
        # подписываем сам график и оси
        plt.title('\nКонверсия в оплату\n', fontsize = 22)
        plt.xlabel("Конверсия, %", size = 14)
        plt.ylabel("Метрика", size = 14)

        # сохраняем график в файл
        plt.savefig('CR.png')

        plt.close()

        # строим график
        splot = sns.barplot(data=metrics_df[['ARPU', 'ARPAU', 'ARPPU', 'grp']]\
                            .melt(id_vars='grp',value_name='value', var_name='metrics'),
                            x='value', y='metrics', hue='grp')
        # подписываем бары на графике
        for g in splot.patches:    
            splot.annotate(format(g.get_width(), '.0f'),  
                (g.get_width(), g.get_y() + g.get_height() / 2.),                   
                ha = 'center', va = 'center',                   
                xytext = (16, 0),                   
                textcoords = 'offset points',
                color = 'grey',
                fontweight = 'bold',
                fontsize = 12)
            
        # убираем границы
        right_side = splot.spines["right"]
        right_side.set_visible(False)
        top_side = splot.spines["top"]
        top_side.set_visible(False)
        
        # подписываем сам график и оси
        plt.title('\nСредний доход на 1 пользователя\n', fontsize = 22)
        plt.xlabel("Средний доход", size = 14)
        plt.ylabel("Метрика\n", size = 14)

        # сохраняем график в файл
        plt.savefig('ARPU_ARPAU_ARPPU.png')

        plt.close()
    
    
    @task()
    def print_log():
        print(f'====== FINAL PROJECT IS DONE======')  
        print(f"====== MESSAGE IS SENT TO TELEGRAM")

    groups_df = get_data(GROUPS_LINK,';')  
    active_studs_df = get_data(ACTIVE_STUDS_LINK)
    checks_df = get_data(CHECKS_LINK, ';')
    
    metrics_df = get_update_data(groups_df, active_studs_df, checks_df)
    
    get_grafics_of_metrics(metrics_df)

    print_log()

final_project_variant_2_a_sibiriakova_15_08 = final_project_variant_2_a_sibiriakova_15_08()
