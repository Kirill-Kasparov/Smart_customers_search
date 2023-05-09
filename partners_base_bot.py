import pandas as pd    # by Kirill Kasparov, 2023, v2.24.02
import os
import telebot
import datetime
import time

# загружаем телебот
with open('bot_token_test.txt', 'r') as file:
    token = file.readline().strip()
bot = telebot.TeleBot(token)
# https://xn--80affa3aj0al.xn--80asehdb/#@sp_base_bot

def cleaner_base():    # Чистим столбцы
    print('Настраиваем форматы столбцов...')
    for col in df.columns:
        df[col] = df[col].fillna('0')
        df[col] = df[col].astype('str')
        df[col] = df[col].str.replace(' ', '')
        df[col] = df[col].str.replace('.0', '', regex=False)
        df[col] = df[col].str.replace(',00', '', regex=False)


# Тело кода
cmd = 'mode 180,30'
os.system(cmd)

# определяем абсолютный путь к файлам
data_import = os.getcwd().replace('\\', '/') + '/' + 'import.csv'
data_export = os.getcwd().replace('\\', '/') + '/' + 'export_partners.csv'
data_export_xlsx = os.getcwd().replace('\\', '/') + '/' + 'export_partners.xlsx'
txt_input = ''

# ищем рабочую базу, обновления, резервную копию
if os.path.exists(data_import):    # проверяем наличие базы данных
    print('Загружаем базу данных...')
    df = pd.read_csv(data_import, sep=';', encoding='windows-1251', dtype='unicode', nrows=1000000)  # загружаем базу
    new_base = []
    if os.path.exists("\\\TU-AO-FS-1/UserArea/Архив/ОКП/АО/База/"):    # Прогоняем поиск обновлений базы
        for root, dirs, files in os.walk("//TU-AO-FS-1/UserArea/Архив/ОКП/АО/База/"):  # такой вариант кода позволяет обойти весь каталог
            for file in files:
                if file.endswith(".xlsb") and ('база' in file.lower()):
                    new_base.append(os.path.join(root) + '/' + os.path.join(file))
        if len(new_base) > 0:
            new_base = max(new_base, key=os.path.getctime)    # получаем последнюю версию
            if os.path.getctime(data_import) < os.path.getctime(new_base) and os.path.getsize(new_base) > 50000000:    # сравниваем последнюю версию с текущей
                print('update: Найдена новая версия базы: ', new_base.split('/')[-1])
                print('update: Для обновления базы введите команду "update", обновление займет около 5 минут.')
    cleaner_base()
    print('База данных загружена, программа готова к работе. Для вывода краткой инструкции по работе с программой, введите "info".')
elif os.path.exists('\\\TU-AO-FS-1/UserArea/Каспаров/Программы/smart_partners_base/import.csv'):
    print('Локальная база данных не найдена. Загружаем новую базу данных...')
    import shutil
    shutil.copyfile('\\\TU-AO-FS-1/UserArea/Каспаров/Программы/smart_partners_base/import.csv', data_import)
    print('Загружаем установленную базу данных...')
    df = pd.read_csv(data_import, sep=';', encoding='windows-1251', dtype='unicode', nrows=1000000)  # загружаем базу
    cleaner_base()
    print('База данных загружена, программа готова к работе. Для вывода краткой инструкции по работе с программой, введите "info".')
else:
    print('База данных не найдена')
    txt_input = 0
    df = False

# Загрузка списка разрешенных пользователей
users_list_df = pd.read_csv('users_list.csv', sep=';', encoding='windows-1251', dtype='unicode', nrows=1000)
admin = '1294387514'
count_logs = 0

@bot.message_handler(content_types=['text'])
def get_text_messages(message, df=df, users_list_df=users_list_df):

    # Главная функция
    def filter_txt(txt):  # !Главная функция!
        if txt[0].isdigit():  # если одно значение и число
            if 8 >= len(txt) > 4:
                return main_partners(txt)  # поиск главкода по коду
            elif 13 > len(txt) > 8:
                return inn_partners(txt)  # поиск по ИНН
            elif 5 > len(txt):
                return network_partners(txt)  # поиск по коду сети
            else:
                bot.send_message(message.from_user.id, 'Ничего не нашли. Перепроверьте цифры')
                return df['Код клиента (120501)'] == '='  # заглушка для False
        else:
            if txt[0] == 'трп' or txt[0] == 'trp' or txt[0] == 'nhg' or txt[0] == 'екз':
                return trp_partners(txt)
            elif txt[0] == 'тп' or txt[0] == 'tp' or txt[0] == 'ng' or txt[0] == 'ез':
                return tp_partners(txt)
            elif txt[0] == 'цфу' or txt[0] == 'cfu' or txt[0] == 'cfy' or txt[0] == 'wae' or txt[0] == 'саг':
                return cfu_partners(txt)
            elif txt[0] == 'вд' or txt[0] == 'vd' or txt[0] == 'dl' or txt[0] == 'мд':
                return vd_partners(txt)
            elif txt[0] == 'увд' or txt[0] == 'uvd' or txt[0] == 'yvd' or txt[0] == 'edl' or txt[0] == 'гмв':
                return uvd_partners(txt)
            elif txt[0] == 'бр' or txt[0] == 'br' or txt[0] == ',h' or txt[0] == 'ик':
                return br_partners(txt)
            elif txt[0] == 'цп' or txt[0] == 'cp' or txt[0] == 'wg' or txt[0] == 'сз' or txt[0] == 'оп' or txt[
                0] == 'op':
                return cp_partners(txt)
            elif txt[0] == 'гп' or txt[0] == 'gp' or txt[0] == 'ug' or txt[0] == 'пз':
                return gp_partners(txt)
            elif txt[0] == 'сап' or txt[0] == 'ид' or txt[0] == 'sap' or txt[0] == 'id' or txt[0] == 'cfg' or txt[
                0] == 'bl' or txt[0] == 'ыфз' or txt[0] == 'шв':
                return sap_id_partners(txt)
            elif txt[0] == 'сап' or txt[0] == 'ид' or txt[0] == 'sap' or txt[0] == 'id' or txt[0] == 'cfg' or txt[
                0] == 'bl' or txt[0] == 'ыфз' or txt[0] == 'шв':
                return sap_id_partners(txt)
            elif txt[0] == 'net' or txt[0] == 'sety' or txt[0] == 'туе' or txt[0] == 'сети':
                return network_partners(txt)
            elif mode == 'equal' and (
                    txt[0] == 'гк' or txt[0] == 'главкод' or txt[0] == 'gk' or txt[0] == 'glavcode' or txt[0] == 'ur' or
                    txt[0] == 'glavkode'):
                return unique_main_partners(txt)
            elif txt[0] == 'инфо' or txt[0] == 'info' or txt[0] == 'byaj' or txt[0] == 'штащ' or txt[0] == '/start':
                return about_program(txt)
            elif os.path.exists("\\\TU-AO-FS-1/UserArea/Архив/ОКП/АО/База/") and txt[0] == 'update' and str(message.from_user.id) == admin:
                return updade_base(txt)
            elif txt[0] == '/logs_pls' and str(message.from_user.id) == admin:
                return admin_logs()
            elif txt[0] == '/new_users_pls' and str(message.from_user.id) == admin:
                return admin_new_users()
            elif txt[0] == '/users_list_pls' and str(message.from_user.id) == admin:
                return admin_users_list()
            elif txt[0] == 'add_user' and str(message.from_user.id) == admin:
                return admin_add_user(txt)
            elif txt[0] == 'restart_pls' and str(message.from_user.id) == admin:
                return admin_restart()
            elif txt[0] == '/crash_logs_pls' and str(message.from_user.id) == admin:
                return admin_crash_logs()
            elif txt[0] == 'shutdown_pls' and str(message.from_user.id) == admin:
                return admin_shutdown()
            elif txt[0] == 'admin' and str(message.from_user.id) == admin:
                return admin_info()
            elif txt[0] == 'komus':
                return komus_authorization()
            else:
                return name_partners(txt)
    # Фундаментальные функции
    def save_mode_selection(txt_list, mode='write'):
        if txt_list[0] == '+':
            mode = 'append'  # меняет режим записи на append - дополнить список
            del txt_list[0]
        elif '+' in txt_list[0]:  # если + указан слитно
            mode = 'append'
            txt_list[0] = txt_list[0].replace('+', '')
        elif txt_list[
            0] == '=':  # меняет режим записи на equal - равный. Отсеивает все, кроме заданного значения, в раннее сформированном списке
            mode = 'equal'  # меняет режим записи на equal
            del txt_list[0]
        elif '=' in txt_list[0]:  # если = указан слитно
            mode = 'equal'
            txt_list[0] = txt_list[0].replace('=', '')
        elif txt_list[0] == '-':  # меняет режим записи на delete - удалить.
            mode = 'delete'  # меняет режим записи на equal
            del txt_list[0]
        elif '-' in txt_list[0]:  # если = указан слитно
            mode = 'delete'
            txt_list[0] = txt_list[0].replace('-', '')
        return [txt_list, mode]
    def save_mode(mode):  # по умолчанию экспортируемый файл перезаписывается. Функция save_mode меняет эти настройки
        print_for_exe = df[mask].to_string(max_colwidth=20, index=False,
                                           columns=['Основной клиент (120510)', 'Наименование осн. клиента (1205101)',
                                                    'Код клиента (120501)', 'Название клиента (12050102)',
                                                    'ИНН (120502)',
                                                    'Код сети  (120535)', 'Назв. Бизнес-рег. отд.тек.(1254221)',
                                                    'Наим. центра прибыли (1254121)', 'Наименование отдела (1254021)',
                                                    'ТРП (120507)', 'Имя ТП (1205041)'],
                                           header=['Главкод', 'Наименование ГК', 'Код кл.', 'Название кл.', 'ИНН',
                                                   'Код сети',
                                                   'БР', 'ЦП', 'ГП', 'ТРП', 'ФИО ТП'])

        print_for_bot = df[mask].to_string(max_colwidth=20, index=False,
                                           columns=['Основной клиент (120510)',
                                                    'Код клиента (120501)', 'Название клиента (12050102)'],
                                           header=['Главкод', 'Код кл.', 'Название кл.'])

        # логи
        logs()

        if mode == 'write' or mode == 'equal':
            if sum(mask) == 0:
                bot.send_message(message.from_user.id, 'Данные не найдены. Для вывода справки отправьте "инфо"')
            else:

                while True:  # проверка, если файл открыт
                    try:
                        df[mask].to_csv(data_export, sep=';', encoding='windows-1251', index=False, mode='w')
                        break
                    except IOError:
                        input("Для сохранения данных необходимо закрыть файл export_partners")
                df[mask].to_csv(data_export, sep=';', encoding='windows-1251', index=False, mode='w')
                df[mask].to_excel(data_export_xlsx, index=False)

                # отправка в чат
                if sum(mask) > 30:
                    if str(message.from_user.id) in list(users_list_df['users']):
                        bot.send_message(message.from_user.id, 'Больше 30 карт. Ловите файл!')
                        f = open(data_export_xlsx, "rb")
                        bot.send_document(message.chat.id, f)
                    else:
                        bot.send_message(message.from_user.id, 'Чтобы получить доступ, отправьте "komus" и вашу почту в чат')
                        bot.send_message(message.from_user.id, 'Пример: komus kks23@komus.net')
                else:
                    bot.send_message(message.from_user.id, print_for_bot)
                    if str(message.from_user.id) in list(users_list_df['users']):
                        f = open(data_export_xlsx, "rb")
                        bot.send_document(message.chat.id, f)
                bot.send_message(message.from_user.id, 'Найдено строк: ' + str(sum(mask)))

        elif mode == 'append':
            if sum(mask) == 0:
                bot.send_message(message.from_user.id, 'Данные не найдены. Для вывода справки отправьте "инфо"')
            else:
                while True:  # проверка, если файл открыт
                    try:
                        df[mask].to_csv(data_export, sep=';', encoding='windows-1251', index=False, mode='a',
                                        header=False)
                        break
                    except IOError:
                        input("Для сохранения данных необходимо закрыть файл export_partners")

                # отправка в чат
                if sum(mask) > 30:
                    if str(message.from_user.id) in list(users_list_df['users']):
                        bot.send_message(message.from_user.id, 'Больше 30 карт. Ловите файл!')
                        f = open(data_export, "rb")
                        bot.send_document(message.chat.id, f)
                    else:
                        bot.send_message(message.from_user.id, 'Чтобы получить доступ, отправьте "komus" и вашу почту в чат')
                        bot.send_message(message.from_user.id, 'Пример: komus kks23@komus.net')

                else:
                    bot.send_message(message.from_user.id, print_for_bot)
                    if str(message.from_user.id) in list(users_list_df['users']):
                        f = open(data_export, "rb")
                        bot.send_document(message.chat.id, f)
                bot.send_message(message.from_user.id, 'Найдено строк: ' + str(sum(mask)))

        elif mode == 'delete':
            if sum(mask) == 0:
                bot.send_message(message.from_user.id, 'Вы нашли секретную функцию! Но она отключена :-)')
                logs()
            else:
                while True:  # проверка, если файл открыт
                    try:
                        df[~mask].to_csv(data_export, sep=';', encoding='windows-1251', index=False, mode='w')
                        break
                    except IOError:
                        input("Для изменения данных необходимо закрыть файл export_partners")
                #print('Данные удалены из файла: ', data_export)
    # Функции поиска
    def main_partners(txt):  # поиск главкода по коду
        mask = df['Код клиента (120501)'] == txt
        main = df['Основной клиент (120510)'][mask]
        if len(main) > 0:
            mask = df['Основной клиент (120510)'] == main.values[0]
        else:
            bot.send_message(message.from_user.id, 'Код Elite не найден')
            logs()
            return df['Код клиента (120501)'] == '='  # заглушка для False
        return mask
    def inn_partners(txt):  # поиск по ИНН
        mask = df['ИНН (120502)'] == txt
        if sum(mask) == 0:
            bot.send_message(message.from_user.id, 'ИНН партнера не найден')
            logs()
            return df['Код клиента (120501)'] == '='  # заглушка для False
        return mask
    def network_partners(txt):  # поиск по коду сети
        if txt[0] == 'net' or txt[0] == 'sety' or txt[0] == 'туе' or txt[0] == 'сети':
            mask = df['Код сети  (120535)'] != '0'
        else:
            mask = df['Код сети  (120535)'] == txt
            if sum(mask) == 0:
                bot.send_message(message.from_user.id, 'Код сети не найден')
                logs()
                return df['Код клиента (120501)'] == '='  # заглушка для False
        return mask
    def trp_partners(txt):  # поиск по логину руководителя группы
        if len(txt) == 1 and mode == 'write' and (txt[0] == 'трп' or txt[0] == 'trp' or txt[0] == 'nhg'):
            mask = df['ТРП (120507)'] + ';' + df['Назв. Бизнес-рег. отд.тек.(1254221)'] + ';' + df[
                'Наим. центра прибыли (1254121)'] + ';' + df['Наименование отдела (1254021)'] + ';' + df[
                       'Код Отдела (125402)']
            mask_unique = pd.DataFrame(mask.unique())  # создаем Серию из уникальных строк
            mask_unique = mask_unique[0].str.split(';', expand=True)
            mask_unique.columns = ['Логин ТРП', 'БР', 'ЦП', 'ГП', 'ЦФУ']

            while True:  # проверка, если файл открыт
                try:
                    mask_unique.to_csv(data_export, sep=';', encoding='windows-1251', index=False)
                    # ответ бота
                    if str(message.from_user.id) in list(users_list_df['users']):
                        bot.send_message(message.from_user.id, 'Список ТРП собран:')
                        f = open(data_export, "rb")
                        bot.send_document(message.chat.id, f)
                        logs()
                    else:
                        bot.send_message(message.from_user.id, 'Чтобы получить доступ, отправьте "komus" и вашу почту в чат')
                        bot.send_message(message.from_user.id, 'Пример: komus kks23@komus.net')

                    break
                except IOError:
                    input("Для сохранения данных необходимо закрыть файл export_partners")

            return df['Код клиента (120501)'] == '='  # заглушка для False
        elif len(txt) > 1 and (txt[0] == 'трп' or txt[0] == 'trp' or txt[0] == 'nhg'):
            mask = df['Код клиента (120501)'] == '='  # заглушка для False
            for log_trp in txt:
                mask += df['ТРП (120507)'] == log_trp
            if sum(mask) == 0:
                bot.send_message(message.from_user.id, 'ТРП не найден')
                logs()
            return mask
        else:
            bot.send_message(message.from_user.id, 'Логин ТРП не найден')
            logs()
            return df['Код клиента (120501)'] == '='  # заглушка для False
    def tp_partners(txt):  # поиск по ТП, формат ввода всегда должен начинаться с "ТП"
        if len(txt) == 1 and mode == 'write' and (
                txt[0] == 'тп' or txt[0] == 'tp' or txt[0] == 'ng'):  # выводит весь список уникальных ТП
            mask = df['Торговый представитель (120504)'] + ';' + df['Имя ТП (1205041)'] + ';' + df[
                'Назв. Бизнес-рег. отд.тек.(1254221)'] + ';' + df['Наим. центра прибыли (1254121)'] + ';' + df[
                       'Наименование отдела (1254021)'] + ';' + df['Код Отдела (125402)']
            mask_unique = pd.DataFrame(mask.unique())  # создаем Серию из уникальных строк
            mask_unique = mask_unique[0].str.split(';', expand=True)
            mask_unique.columns = ['Код ТП', 'ФИО ТП', 'БР', 'ЦП', 'ГП', 'ЦФУ']
            mask_unique = mask_unique.sort_values(by=['БР', 'ЦП'])  # сортируем по БР

            while True:  # проверка, если файл открыт
                try:
                    mask_unique.to_csv(data_export, sep=';', encoding='windows-1251', index=False)
                    # ответ бота
                    if str(message.from_user.id) in list(users_list_df['users']):
                        bot.send_message(message.from_user.id, 'Список ТП собран:')
                        f = open(data_export, "rb")
                        bot.send_document(message.chat.id, f)
                        logs()
                    else:
                        bot.send_message(message.from_user.id, 'Чтобы получить доступ, отправьте "komus" и вашу почту в чат')
                        bot.send_message(message.from_user.id, 'Пример: komus kks23@komus.net')

                    break
                except IOError:
                    input("Для сохранения данных необходимо закрыть файл export_partners")

            return df['Код клиента (120501)'] == '='  # заглушка для False
        elif len(txt) > 1 and txt[1].isdigit() and (
                txt[0] == 'тп' or txt[0] == 'tp' or txt[0] == 'ng'):  # поиск ТП по коду, можно несколько через пробел
            mask = df['Код клиента (120501)'] == '='  # заглушка False
            for log_tp in txt:  # перебираем все коды
                mask += df['Торговый представитель (120504)'] == log_tp
            if sum(mask) == 0:
                bot.send_message(message.from_user.id, 'ТП не найден')
                logs()
            return mask
        elif len(txt) > 1 and (txt[0] == 'тп' or txt[0] == 'tp' or txt[0] == 'ng'):
            txt = ' '.join(txt[1:]).split(',')  # пересобираем список: убираем ТП, соединяем ФИО с разделителем запятая
            df_tp = df['Имя ТП (1205041)'].str.lower()  # нижний регистр для поиска
            mask = df['Код клиента (120501)'] == '='  # заглушка False
            for name_tp in txt:  # цикл, если указано несколько ТР через запятую
                mask += df_tp.str.contains(name_tp.strip(), regex=False)  # удаляем лишние пробелы и собираем маску
            if sum(mask) == 0:
                bot.send_message(message.from_user.id, 'ТП не найден')
                logs()
            return mask
        else:
            bot.send_message(message.from_user.id, 'ТП не найден')
            logs()
            return df['Код клиента (120501)'] == '='  # заглушка для False
    def cfu_partners(txt):  # Поиск по коду ЦФУ
        if len(txt) == 1 and mode == 'write' and (txt[0] == 'цфу' or txt[0] == 'cfu' or txt[0] == 'cfy' or txt[
            0] == 'wae'):  # выводит весь список уникальных ТП
            mask = df['Код Отдела (125402)'] + ';' + df['Назв. Бизнес-рег. отд.тек.(1254221)'] + ';' + df[
                'Наим. центра прибыли (1254121)'] + ';' + df['Наименование отдела (1254021)'] + ';' + df[
                       'Назв. Бизнес-рег. отд.тек.(1254221)'] + '/' + df['Наим. центра прибыли (1254121)'] + '/' + df[
                       'Наименование отдела (1254021)']
            mask_unique = pd.DataFrame(mask.unique())  # создаем Серию из уникальных строк
            mask_unique = mask_unique[0].str.split(';', expand=True)
            mask_unique.columns = ['ЦФУ', 'БР', 'ЦП', 'ГП', 'Сцепка']
            mask_unique = mask_unique.sort_values(by=['БР', 'ЦП'])  # сортируем по БР

            while True:  # проверка, если файл открыт
                try:
                    mask_unique.to_csv(data_export, sep=';', encoding='windows-1251', index=False)
                    # ответ бота
                    if str(message.from_user.id) in list(users_list_df['users']):
                        bot.send_message(message.from_user.id, 'Список ЦФУ собран:')
                        f = open(data_export, "rb")
                        bot.send_document(message.chat.id, f)
                        logs()
                    else:
                        bot.send_message(message.from_user.id, 'Чтобы получить доступ, отправьте "komus" и вашу почту в чат')
                        bot.send_message(message.from_user.id, 'Пример: komus kks23@komus.net')
                    break
                except IOError:
                    input("Для сохранения данных необходимо закрыть файл export_partners")

            return df['Код клиента (120501)'] == '='  # заглушка для False
        elif len(txt) > 1 and (txt[0] == 'цфу' or txt[0] == 'cfu' or txt[0] == 'cfy' or txt[0] == 'wae'):
            mask = df['Код клиента (120501)'] == '='  # заглушка для False
            df_cfu = df['Код Отдела (125402)'].str.lower()  # нижний регистр для поиска
            for log_cfu in txt:
                mask += df_cfu == log_cfu
            if sum(mask) == 0:
                bot.send_message(message.from_user.id, 'ЦФУ не найден')
                logs()
            return mask
        else:
            bot.send_message(message.from_user.id, 'ЦФУ не найден')
            logs()
            return df['Код клиента (120501)'] == '='  # заглушка для False
    def vd_partners(txt):  # поиск по ВД
        if len(txt) == 1 and mode == 'write' and (
                txt[0] == 'вд' or txt[0] == 'vd' or txt[0] == 'dl'):  # выводит весь список уникальных ВД
            mask = df['Название основного в.д  (120523)'] + ';' + df['Название уточняющего в.д  (120525)']
            mask_unique = pd.DataFrame(mask.unique())  # создаем Серию из уникальных строк
            mask_unique = mask_unique[0].str.split(';', expand=True)  # разделяем столбцы уникальных значений
            mask_unique = mask_unique.dropna()  # удаляем пропуски
            mask_unique.columns = ['ВД', 'УВД']
            mask_unique = mask_unique.sort_values(by='ВД')  # сортируем по ВД

            while True:  # проверка, если файл открыт
                try:
                    mask_unique.to_csv(data_export, sep=';', encoding='windows-1251', index=False)
                    # ответ бота
                    if str(message.from_user.id) in list(users_list_df['users']):
                        bot.send_message(message.from_user.id, 'Список ВД собран:')
                        f = open(data_export, "rb")
                        bot.send_document(message.chat.id, f)
                        logs()
                    else:
                        bot.send_message(message.from_user.id, 'Чтобы получить доступ, отправьте "komus" и вашу почту в чат')
                        bot.send_message(message.from_user.id, 'Пример: komus kks23@komus.net')
                    break
                except IOError:
                    input("Для сохранения данных необходимо закрыть файл export_partners")

            return df['Код клиента (120501)'] == '='  # заглушка для False
        elif len(txt) > 1 and (txt[0] == 'вд' or txt[0] == 'vd' or txt[0] == 'dl'):
            txt = ' '.join(txt[1:]).split(
                ',')  # пересобираем список: соединяем текст между запятыми, запятые используем как разделители
            df_vd = df['Название основного в.д  (120523)'].str.lower()  # нижний регистр для поиска
            mask = df['Код клиента (120501)'] == '='  # заглушка False
            for name_vd in txt:  # цикл, если указано несколько ТР через запятую
                mask += df_vd.str.contains(name_vd.strip(), regex=False)  # удаляем лишние пробелы и собираем маску
            if sum(mask) == 0:
                bot.send_message(message.from_user.id, 'ВД не найден')
                logs()
            return mask
        else:
            bot.send_message(message.from_user.id, 'ВД не найден')
            logs()
            return df['Код клиента (120501)'] == '='  # заглушка для False
    def uvd_partners(txt):  # поиск по уточняющему ВД
        if len(txt) == 1 and mode == 'write' and (txt[0] == 'увд' or txt[0] == 'uvd' or txt[0] == 'yvd' or txt[
            0] == 'edl'):  # выводит весь список уникальных УВД
            mask = df['Название основного в.д  (120523)'] + ';' + df['Название уточняющего в.д  (120525)']
            mask_unique = pd.DataFrame(mask.unique())  # создаем Серию из уникальных строк
            mask_unique = mask_unique[0].str.split(';', expand=True)  # разделяем столбцы уникальных значений
            mask_unique = mask_unique.dropna()  # удаляем пропуски
            mask_unique.columns = ['ВД', 'УВД']
            mask_unique = mask_unique.sort_values(by='ВД')  # сортируем по ВД

            while True:  # проверка, если файл открыт
                try:
                    mask_unique.to_csv(data_export, sep=';', encoding='windows-1251', index=False)
                    # ответ бота
                    if str(message.from_user.id) in list(users_list_df['users']):
                        bot.send_message(message.from_user.id, 'Список УВД собран:')
                        f = open(data_export, "rb")
                        bot.send_document(message.chat.id, f)
                        logs()
                    else:
                        bot.send_message(message.from_user.id, 'Чтобы получить доступ, отправьте "komus" и вашу почту в чат')
                        bot.send_message(message.from_user.id, 'Пример: komus kks23@komus.net')
                    break
                except IOError:
                    input("Для сохранения данных необходимо закрыть файл export_partners")

            return df['Код клиента (120501)'] == '='  # заглушка для False
        elif len(txt) > 1 and (txt[0] == 'увд' or txt[0] == 'uvd' or txt[0] == 'yvd'or txt[0] == 'edl'):
            txt = ' '.join(txt[1:]).split(
                ',')  # пересобираем список: соединяем текст между запятыми, запятые используем как разделители
            df_vd = df['Название уточняющего в.д  (120525)'].str.lower()  # нижний регистр для поиска
            mask = df['Код клиента (120501)'] == '='  # заглушка False
            for name_vd in txt:  # цикл, если указано несколько ТР через запятую
                mask += df_vd.str.contains(name_vd.strip(), regex=False)  # удаляем лишние пробелы и собираем маску
            if sum(mask) == 0:
                bot.send_message(message.from_user.id, 'УВД не найден')
                logs()
            return mask
        else:
            bot.send_message(message.from_user.id, 'УВД не найден')
            logs()
            return df['Код клиента (120501)'] == '='  # заглушка для False
    def br_partners(txt):  # поиск по бизнес-региону
        if len(txt) == 1 and mode == 'write' and (
                txt[0] == 'бр' or txt[0] == 'br' or txt[0] == ',h'):  # выводит весь список уникальных БР
            mask = df['Назв. Бизнес-рег. отд.тек.(1254221)'] + ';' + df['Название региона отд.тек. (1254161)']
            mask_unique = pd.DataFrame(mask.unique())  # создаем Серию из уникальных строк
            mask_unique = mask_unique[0].str.split(';', expand=True)  # разделяем столбцы уникальных значений
            mask_unique = mask_unique.dropna()  # удаляем пропуски
            mask_unique.columns = ['БР', 'Регион']
            mask_unique = mask_unique.sort_values(by='БР')  # сортируем по БР

            while True:  # проверка, если файл открыт
                try:
                    mask_unique.to_csv(data_export, sep=';', encoding='windows-1251', index=False)
                    # ответ бота
                    if str(message.from_user.id) in list(users_list_df['users']):
                        bot.send_message(message.from_user.id, 'Список структуры по БР собран:')
                        f = open(data_export, "rb")
                        bot.send_document(message.chat.id, f)
                        logs()
                    else:
                        bot.send_message(message.from_user.id, 'Чтобы получить доступ, отправьте "komus" и вашу почту в чат')
                        bot.send_message(message.from_user.id, 'Пример: komus kks23@komus.net')
                    break
                except IOError:
                    input("Для сохранения данных необходимо закрыть файл export_partners")

            return df['Код клиента (120501)'] == '='  # заглушка для False
        elif len(txt) > 1 and (txt[0] == 'бр' or txt[0] == 'br'):
            txt = ' '.join(txt[1:]).split(
                ',')  # пересобираем список: соединяем текст между запятыми, запятые используем как разделители
            df_br = df['Назв. Бизнес-рег. отд.тек.(1254221)'].str.lower()  # нижний регистр для поиска
            mask = df['Код клиента (120501)'] == '='  # заглушка False
            for name_br in txt:  # цикл, если указано несколько ТР через запятую
                mask += df_br.str.contains(name_br.strip(), regex=False)  # удаляем лишние пробелы и собираем маску
            if sum(mask) == 0:
                bot.send_message(message.from_user.id, 'БР не найден')
                logs()
            return mask
        else:
            bot.send_message(message.from_user.id, 'БР не найден')
            logs()
            return df['Код клиента (120501)'] == '='  # заглушка для False
    def cp_partners(txt):  # поиск по центру прибыли
        if len(txt) == 1 and mode == 'write':  # выводит весь список уникальных БР
            mask = df['Наим. центра прибыли (1254121)'] + ';' + df['Код центра прибыли (125412)'] + ';' + df[
                'Название региона отд.тек. (1254161)'] + ';' + df['Назв. Бизнес-рег. отд.тек.(1254221)']
            mask_unique = pd.DataFrame(mask.unique())  # создаем Серию из уникальных строк
            mask_unique = mask_unique[0].str.split(';', expand=True)  # разделяем столбцы уникальных значений
            mask_unique = mask_unique.dropna()  # удаляем пропуски
            mask_unique.columns = ['ЦП', 'Код ЦП', 'Регион', 'БР']
            mask_unique = mask_unique.sort_values(by=['БР', 'ЦП'])  # сортируем по БР

            while True:  # проверка, если файл открыт
                try:
                    mask_unique.to_csv(data_export, sep=';', encoding='windows-1251', index=False)
                    # ответ бота
                    if str(message.from_user.id) in list(users_list_df['users']):
                        bot.send_message(message.from_user.id, 'Список структуры по ЦП собран:')
                        f = open(data_export, "rb")
                        bot.send_document(message.chat.id, f)
                        logs()
                    else:
                        bot.send_message(message.from_user.id, 'Чтобы получить доступ, отправьте "komus" и вашу почту в чат')
                        bot.send_message(message.from_user.id, 'Пример: komus kks23@komus.net')
                    break
                except IOError:
                    input("Для сохранения данных необходимо закрыть файл export_partners")

            return df['Код клиента (120501)'] == '='  # заглушка для False
        elif len(txt) > 1:
            txt = ' '.join(txt[1:]).split(
                ',')  # пересобираем список: соединяем текст между запятыми, запятые используем как разделители
            df_br = df['Наим. центра прибыли (1254121)'].str.lower()  # нижний регистр для поиска
            mask = df['Код клиента (120501)'] == '='  # заглушка False
            for name_br in txt:  # цикл, если указано несколько ТР через запятую
                mask += df_br.str.contains(name_br.strip(), regex=False)  # удаляем лишние пробелы и собираем маску
            if sum(mask) == 0:  # если указан код ЦП, а не название, то продолжит поиск по коду ЦП
                df['Код центра прибыли (125412)'] = df['Код центра прибыли (125412)'].fillna('0')
                df['Код центра прибыли (125412)'] = df['Код центра прибыли (125412)'].astype('str')
                df_br = df['Код центра прибыли (125412)'].str.lower()  # нижний регистр для поиска
                for name_br in txt:  # цикл, если указано несколько ТР через запятую
                    mask += df_br.str.contains(name_br.strip(), regex=False)  # удаляем лишние пробелы и собираем маску
                if sum(mask) == 0:
                    bot.send_message(message.from_user.id, 'ЦП не найден')
                    logs()
            return mask
        else:
            bot.send_message(message.from_user.id, 'ЦП не найден')
            logs()
            return df['Код клиента (120501)'] == '='  # заглушка для False
    def gp_partners(txt):  # поиск по группе продаж
        if len(txt) == 1 and mode == 'write' and (
                txt[0] == 'гп' or txt[0] == 'gp' or txt[0] == 'ug'):  # выводит весь список уникальных ГП
            mask = df['Наименование отдела (1254021)'] + ';' + df['Код Отдела (125402)'] + ';' + df[
                'Наим. центра прибыли (1254121)'] + ';' + df['Код центра прибыли (125412)'] + ';' + df[
                       'Название региона отд.тек. (1254161)'] + ';' + df['Назв. Бизнес-рег. отд.тек.(1254221)']
            mask_unique = pd.DataFrame(mask.unique())  # создаем Серию из уникальных строк
            mask_unique = mask_unique[0].str.split(';', expand=True)  # разделяем столбцы уникальных значений
            mask_unique = mask_unique.dropna()  # удаляем пропуски
            mask_unique.columns = ['ГП', 'Код ЦФУ', 'ЦП', 'Код ЦП', 'Регион', 'БР']
            mask_unique = mask_unique.sort_values(by=['БР', 'ЦП', 'ГП'])  # сортируем по БР

            while True:  # проверка, если файл открыт
                try:
                    mask_unique.to_csv(data_export, sep=';', encoding='windows-1251', index=False)
                    # ответ бота
                    if str(message.from_user.id) in list(users_list_df['users']):
                        bot.send_message(message.from_user.id, 'Структура по ГП собрана:')
                        f = open(data_export, "rb")
                        bot.send_document(message.chat.id, f)
                        logs()
                    else:
                        bot.send_message(message.from_user.id, 'Чтобы получить доступ, отправьте "komus" и вашу почту в чат')
                        bot.send_message(message.from_user.id, 'Пример: komus kks23@komus.net')
                    break
                except IOError:
                    input("Для сохранения данных необходимо закрыть файл export_partners")

            return df['Код клиента (120501)'] == '='  # заглушка для False
        elif len(txt) > 1 and (txt[0] == 'гп' or txt[0] == 'gp' or txt[0] == 'ug'):
            txt = ' '.join(txt[1:]).split(
                ',')  # пересобираем список: соединяем текст между запятыми, запятые используем как разделители
            df_br = df['Наименование отдела (1254021)'].str.lower()  # нижний регистр для поиска
            mask = df['Код клиента (120501)'] == '='  # заглушка False
            for name_br in txt:  # цикл, если указано несколько ТР через запятую
                mask += df_br.str.contains(name_br.strip(), regex=False)  # удаляем лишние пробелы и собираем маску
            if sum(mask) == 0:  # если указан код ГП, а не название, то продолжит поиск по коду ЦФУ этой ГП
                df['Код Отдела (125402)'] = df['Код Отдела (125402)'].fillna('0')
                df['Код Отдела (125402)'] = df['Код Отдела (125402)'].astype('str')
                df_br = df['Код Отдела (125402)'].str.lower()  # нижний регистр для поиска
                for name_br in txt:  # цикл, если указано несколько ТР через запятую
                    mask += df_br.str.contains(name_br.strip(), regex=False)  # удаляем лишние пробелы и собираем маску
                if sum(mask) == 0:
                    bot.send_message(message.from_user.id, 'ГП не найдена')
                    logs()
            return mask
        else:
            print('Запрошенные данные в поле ГП приведут к ошибке. Пожалуйста, сформулируйте запрос иначе.')
            return df['Код клиента (120501)'] == '='  # заглушка для False
    def name_partners(txt):  # поиск по наименованию партнера
        txt = ' '.join(txt).split(
            ',')  # пересобираем список: соединяем текст между запятыми, запятые используем как разделители
        df_name = df['Название клиента (12050102)'].str.lower()  # нижний регистр для поиска
        mask = df['Код клиента (120501)'] == '='  # заглушка False
        for name_txt in txt:  # цикл, если указано несколько наименований через запятую
            mask += df_name.str.contains(name_txt.strip(), regex=False)  # удаляем лишние пробелы и собираем маску
        if sum(mask) == 0:  # если нет результата поиска
            bot.send_message(message.from_user.id, 'Партнеров с указанным наименованием не найдено')
            logs()
        return mask
    def sap_id_partners(txt):  # поиск по коду sap id
        mask = df['Код клиента (120501)'] == '='  # заглушка False
        if len(txt) > 1:
            for id in txt:
                mask += df['Код партнера в SAP (120551)'] == id
            if sum(mask) == 0:
                bot.send_message(message.from_user.id, 'SAP id не найден')
                logs()
            return mask
        else:
            bot.send_message(message.from_user.id, 'Не указан SAP id. Пример запроса: сап 3870775 1906719: ')
            logs()
            return df['Код клиента (120501)'] == '='  # заглушка для False
    def unique_main_partners(txt):  # оставить только главкоды
        mask = df['Код клиента (120501)'] == df['Основной клиент (120510)']
        return mask
    def about_program(txt):
        bot.send_message(message.from_user.id, 'Описание: эта программа работает как универсальный фильтр поиска по партнерской базе. Реализованы механизмы поиска:')
        bot.send_message(message.from_user.id, 'По коду партнера. Пример запроса: 2830207 1670771 1069153')
        bot.send_message(message.from_user.id, 'По наименованию партнера. Пример запроса: Газпром, Торгсервис')
        bot.send_message(message.from_user.id, 'По ИНН. Пример запроса: 7706564354 7702836198')
        bot.send_message(message.from_user.id, 'По коду сети. Пример запроса: 100 227 2830')
        bot.send_message(message.from_user.id, '«SAP» - по коду SAP id. Пример запроса: сап 3870775 1906719')
        bot.send_message(message.from_user.id, '«ТРП» - по руководителю группы. Пример запроса: ТРП mma12 aen28 baeeq')
        bot.send_message(message.from_user.id, '«ТП» - по торговому представителю. Пример запроса: ТП Иванов Иван, Алексей, Петров, Дмитрий Владимирович')
        bot.send_message(message.from_user.id, '«ЦФУ» - По коду ЦФУ. Пример запроса: ЦФУ 1QE 23L 24H')
        bot.send_message(message.from_user.id, '«ГП» - по названию группы продаж. Пример запроса: ГП Торговля, Производство')
        logs()
        return df['Код клиента (120501)'] == '='  # заглушка для False
    def updade_base(txt):
        bot.send_message(message.from_user.id, 'Загружаем базу: ' + str(new_base))
        df = pd.read_excel(new_base, engine='pyxlsb')
        bot.send_message(message.from_user.id, 'Сохраняем базу на диске...')
        df.to_csv(data_import, sep=';', encoding='windows-1251', index=False, mode='w')
        if os.path.exists("\\\TU-AO-FS-1/UserArea/Каспаров/Программы/smart_partners_base/"):
            if os.path.getctime(data_import) > os.path.getctime(
                    '\\\TU-AO-FS-1/UserArea/Каспаров/Программы/smart_partners_base/import.csv'):
                import shutil
                os.remove('\\\TU-AO-FS-1/UserArea/Каспаров/Программы/smart_partners_base/import.csv')
                shutil.copyfile(data_import, '\\\TU-AO-FS-1/UserArea/Каспаров/Программы/smart_partners_base/import.csv')
        cleaner_base()
        bot.send_message(message.from_user.id, 'Обновление завершено.')
        return df['Код клиента (120501)'] == '='  # заглушка для False
    def cleaner_base():  # Чистим столбцы
        print('Настраиваем форматы столбцов...')
        for col in df.columns:
            df[col] = df[col].fillna('0')
            df[col] = df[col].astype('str')
            df[col] = df[col].str.replace(' ', '')
            df[col] = df[col].str.replace('.0', '', regex=False)
            df[col] = df[col].str.replace(',00', '', regex=False)
    def logs():
        # логи
        user_logs = 'date: ' + str(datetime.datetime.today()) + ';' + 'user:' + str(message.from_user.id) + ';' + \
                    'name:' + str(message.from_user.first_name) + ' ' + str(message.from_user.last_name) + ';' + \
                    'username:' + str(message.from_user.username) + ';' + \
                    'message:' + str(message.text) + ';' + 'result:' + str(sum(mask))
        print(user_logs)
        while True:  # проверка, если файл открыт
            try:
                save_user_logs = pd.Series(user_logs)
                save_user_logs.to_csv('users_logs.csv', sep=';', encoding='windows-1251', index=False, mode='a',
                                      header=False)
                break
            except UnicodeEncodeError:
                break
    def komus_authorization():
        user_access = 'add_user ' + str(datetime.datetime.today()) + ';' + str(message.from_user.id) + ';' + \
                      str(message.from_user.first_name) + ' ' + str(message.from_user.last_name) + ';' + \
                      str(message.from_user.username) + ';' + str(message.text)
        save_user_logs = pd.Series(user_access)
        save_user_logs.to_csv('new_users.csv', sep=';', encoding='windows-1251', index=False, mode='a', header=False)
        bot.send_message(message.from_user.id, 'Запрос направлен модератору чата')
        bot.send_message(message.from_user.id, 'Для просмотра файлов .csv после авторизации, установите программу Microsoft Excel')
        logs()
        return df['Код клиента (120501)'] == '='  # заглушка для False
    def admin_logs():
        bot.send_message(message.from_user.id, 'После прочтения сжечь:')
        f = open('users_logs.csv', "rb")
        bot.send_document(message.chat.id, f)
        logs()
        return df['Код клиента (120501)'] == '='  # заглушка для False
    def admin_new_users():
        bot.send_message(message.from_user.id, 'Новобранцы прибыли')
        f = open('new_users.csv', "rb")
        bot.send_document(message.chat.id, f)
        logs()
        return df['Код клиента (120501)'] == '='  # заглушка для False
    def admin_users_list():
        bot.send_message(message.from_user.id, 'Список строевых:')
        f = open('users_list.csv', "rb")
        bot.send_document(message.chat.id, f)
        logs()
        return df['Код клиента (120501)'] == '='  # заглушка для False
    def admin_add_user(txt, users_list_df=users_list_df):
        user = ' '.join(txt).split(';')
        if len(user) == 5:
            dict = {'users': user[1], 'name': user[2], 'username': user[3], 'message': user[4]}
            users_list_df = users_list_df.append(dict, ignore_index=True)
            users_list_df.to_csv('users_list.csv', sep=';', encoding='windows-1251', index=False, mode='a',
                                 header=False)
            bot.send_message(message.from_user.id, 'Welcome, ' + str(user[2]))
        logs()
        return df['Код клиента (120501)'] == '='  # заглушка для False
    def admin_restart():
        bot.stop_polling()
        crash_logs = pd.Series(str(pd.to_datetime(int(time.time() + 10800), unit='s')) + ';' + 'Рестарт')
        crash_logs.to_csv('crash_logs.csv', sep=';', encoding='windows-1251', index=False, mode='a', header=False)
        os.startfile("Smart Partners base (server for bot).exe")
        bot.send_message(message.from_user.id, 'Перезапуск запущен')
        return df['Код клиента (120501)'] == '='  # заглушка для False
    def admin_crash_logs():
        bot.send_message(message.from_user.id, 'После прочтения сжечь:')
        f = open('crash_logs.csv', "rb")
        bot.send_document(message.chat.id, f)
        logs()
        return df['Код клиента (120501)'] == '='  # заглушка для False
    def admin_shutdown():
        bot.stop_polling()
        crash_logs = pd.Series(str(pd.to_datetime(int(time.time() + 10800), unit='s')) + ';' + 'Выключение')
        crash_logs.to_csv('crash_logs.csv', sep=';', encoding='windows-1251', index=False, mode='a', header=False)
        bot.send_message(message.from_user.id, 'Сладких снов!')
        return df['Код клиента (120501)'] == '='  # заглушка для False
    def admin_info():
        bot.send_message(message.from_user.id, '/logs_pls - логи запросов пользователей')
        bot.send_message(message.from_user.id, '/users_list_pls - список авторизованных пользователей')
        bot.send_message(message.from_user.id, '«komus текст» - запросить авторизацию')
        bot.send_message(message.from_user.id, '/new_users_pls - список запросов на авторизацию')
        bot.send_message(message.from_user.id, '«add_user текст» - авторизовать пользователя')
        bot.send_message(message.from_user.id, '«update» - обновление базы')
        bot.send_message(message.from_user.id, '«restart_pls» - перезапуск программы')
        bot.send_message(message.from_user.id, '/crash_logs_pls - логи перезапусков')
        bot.send_message(message.from_user.id, '«shutdown_pls» - выключение программы')
        logs()
        return df['Код клиента (120501)'] == '='  # заглушка для False

    while True:  # проверка, если файл открыт
        try:
            txt_input = message.text.lower()
            if len(txt_input) != 0:
                break
        except KeyboardInterrupt:
            print(
                "Такой тип ввода приведет к ошибке программы. Попробуйте скопировать данные еще раз, без переносов строк.")
    txt_list = txt_input.split()  # проверяем список

    func1 = save_mode_selection(txt_list)  # функция выбора режима сохранения файла
    txt_list, mode = func1[0], func1[1]
    if mode == 'equal' or mode == 'delete':
        print('Загружаем раннее сформированную базу данных...')
        while True:  # проверка, если файл открыт
            try:
                df2 = pd.read_csv(data_export, sep=';', encoding='windows-1251', low_memory=False,
                                  nrows=1000000)  # загружаем базу
                break
            except IOError:
                input("Для загрузки данных необходимо закрыть файл export_partners")
        df, df2 = df2, df
        print('База данных загружена, обрабатываем запрос.')
    else:
        df2 = False  # убрать ошибку

    mask = df['Код клиента (120501)'] == '='  # сводная маска для нескольких значений
    count_txt_list = 1  # счетчик, чтобы веселее было ждать перебор нескольких значений
    if txt_list[0].replace(',',
                           '').isdigit():  # если первое значение = цифра, передаем по одному и записываем в общую маску
        for txt in txt_list:
            if len(txt_list) > 1:
                print('Введено значений: ', len(txt_list), 'Обработано: ', count_txt_list)
                count_txt_list += 1
            if txt.isdigit():
                mask += filter_txt(txt.replace(',', ''))
            else:
                print(
                    'В переданный список попало нечисловое значение. Программа не работает со смешанным типом данных в одном запросе.')
    else:  # во всех остальных случаях передаем весь список, а не один элемент
        mask += filter_txt(txt_list)

    if txt_input != '0':  # только для возвращенных масок. Исключает уникальные списки-справочники ЦФУ/БР/ГП/ТРП/ТП
        if sum(mask) != 0:
            save_mode(mode)
            if mode == 'equal' or mode == 'delete':
                df, df2 = df2, df
    else:
        print('Bye-bye!')

try:    # перезапуск при достижении лимита и дисконекте
    count_logs += 1
    if count_logs == 200:    # перезапуск при достижении лимита
        bot.stop_polling()
        crash_logs = pd.Series(str(pd.to_datetime(int(time.time() + 10800), unit='s')) + ';' + 'Достигнут лимит 200 запросов')
        crash_logs.to_csv('crash_logs.csv', sep=';', encoding='windows-1251', index=False, mode='a', header=False)
        os.startfile("Smart Partners base (server for bot).exe")

    bot.polling(none_stop=True, interval=0)
except:    # перезапуск при дисконекте
    crash_logs = pd.Series(str(pd.to_datetime(int(time.time() + 10800), unit='s')) + ';' + 'Дисконнект')
    crash_logs.to_csv('crash_logs.csv', sep=';', encoding='windows-1251', index=False, mode='a', header=False)
    time.sleep(10)
    os.startfile("Smart Partners base (server for bot).exe")