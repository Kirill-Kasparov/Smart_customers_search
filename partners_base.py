print('База данных v1.32')    # by Kirill Kasparov, 2022
import pandas as pd
import os

# Главная функция
def filter_txt(txt):    # !Главная функция!
    if txt[0].isdigit():    # если одно значение и число
        if 8 > len(txt) > 4:
            return main_partners(txt)    # поиск главкода по коду
        elif 13 > len(txt) > 8:
            return inn_partners(txt)    # поиск по ИНН
        elif 5 > len(txt):
            return network_partners(txt)    # поиск по коду сети
        else:
            print('Неизвестное значение')
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
        elif txt[0] == 'цп' or txt[0] == 'cp' or txt[0] == 'wg' or txt[0] == 'сз' or txt[0] == 'оп' or txt[0] == 'op':
            return cp_partners(txt)
        elif txt[0] == 'гп' or txt[0] == 'gp' or txt[0] == 'ug' or txt[0] == 'пз':
            return gp_partners(txt)
        elif txt[0] == 'сап' or txt[0] == 'ид' or txt[0] == 'sap' or txt[0] == 'id' or txt[0] == 'cfg' or txt[0] == 'bl' or txt[0] == 'ыфз' or txt[0] == 'шв':
            return sap_id_partners(txt)
        elif txt[0] == 'ул' or txt[0] == 'ул.' or txt[0] == 'адрес':
            return street_partners(txt)
        elif txt[0] == 'г' or txt[0] == 'г.' or txt[0] == 'город':
            return city_partners(txt)
        elif txt[0] == '@' or txt[0] == 'e-mail:':
            return mail_partners(txt)
        elif txt[0] == 'net' or txt[0] == 'sety' or txt[0] == 'туе' or txt[0] == 'сети':
            return network_partners(txt)
        elif mode == 'equal' and (txt[0] == 'гк' or txt[0] == 'главкод' or txt[0] == 'gk' or txt[0] == 'glavcode' or txt[0] == 'ur' or txt[0] == 'glavkode'):
            return unique_main_partners(txt)
        elif txt[0] == 'инфо' or txt[0] == 'info' or txt[0] == 'byaj' or txt[0] == 'штащ':
            return about_program(txt)
        elif os.path.exists("\\\TU-AO-FS-1/UserArea/Архив/ОКП/АО/База/") and txt[0] == 'update':
            return updade_base(txt)
        else:
            return name_partners(txt)
# Фундаментальные функции
def save_mode_selection(txt_list, mode = 'write'):
    if txt_list[0] == '+':
        mode = 'append'    # меняет режим записи на append - дополнить список
        del  txt_list[0]
    elif '+' in txt_list[0]:    # если + указан слитно
        mode = 'append'
        txt_list[0] = txt_list[0].replace('+', '')
    elif txt_list[0] == '=':    # меняет режим записи на equal - равный. Отсеивает все, кроме заданного значения, в раннее сформированном списке
        mode = 'equal'    # меняет режим записи на equal
        del  txt_list[0]
    elif '=' in txt_list[0]:    # если = указан слитно
        mode = 'equal'
        txt_list[0] = txt_list[0].replace('=', '')
    elif txt_list[0] == '-':  # меняет режим записи на delete - удалить.
        mode = 'delete'  # меняет режим записи на equal
        del txt_list[0]
    elif '-' in txt_list[0]:  # если = указан слитно
        mode = 'delete'
        txt_list[0] = txt_list[0].replace('-', '')
    return [txt_list, mode]
def save_mode(mode):    # по умолчанию экспортируемый файл перезаписывается. Функция save_mode меняет эти настройки
    print_for_exe = df[mask].to_string(max_colwidth=20, index=False,
                                 columns=['Основной клиент (120510)', 'Наименование осн. клиента (1205101)',
                                          'Код клиента (120501)', 'Название клиента (12050102)', 'ИНН (120502)',
                                          'Код сети  (120535)', 'Назв. Бизнес-рег. отд.тек.(1254221)',
                                          'Наим. центра прибыли (1254121)', 'Наименование отдела (1254021)',
                                          'ТРП (120507)', 'Имя ТП (1205041)'],
                                 header=['Главкод', 'Наименование ГК', 'Код кл.', 'Название кл.', 'ИНН', 'Код сети',
                                         'БР', 'ЦП', 'ГП', 'ТРП', 'ФИО ТП'])
    #print_for_exe = df[mask].to_string(max_colwidth=20, index=False)
    if mode == 'write' or mode == 'equal':
        if sum(mask) == 0:
            print('Данные не найдены')
        else:
            print(print_for_exe)
            print('Найдено строк: ', sum(mask))
            while True:  # проверка, если файл открыт
                try:
                    df[mask].to_csv(data_export, sep=';', encoding='windows-1251', index=False, mode='w')
                    break
                except IOError:
                    input("Для сохранения данных необходимо закрыть файл export_partners")
            df[mask].to_csv(data_export, sep=';', encoding='windows-1251', index=False, mode='w')
            print('Данные сохранены в файл: ', data_export)
    elif mode == 'append':
        if sum(mask) == 0:
            print('Данные не найдены')
        else:
            print(print_for_exe)
            print('Найдено строк: ', sum(mask))
            while True:  # проверка, если файл открыт
                try:
                    df[mask].to_csv(data_export, sep=';', encoding='windows-1251', index=False, mode='a', header=False)
                    break
                except IOError:
                    input("Для сохранения данных необходимо закрыть файл export_partners")
            print('Новые данные добавлены в файл: ', data_export)
    elif mode == 'delete':
        if sum(mask) == 0:
            print('Данные не найдены')
        else:
            print(df[~mask].to_string(max_colwidth=20, index=False,
                                 columns=['Основной клиент (120510)', 'Наименование осн. клиента (1205101)',
                                          'Код клиента (120501)', 'Название клиента (12050102)', 'ИНН (120502)',
                                          'Код сети  (120535)', 'Назв. Бизнес-рег. отд.тек.(1254221)',
                                          'Наим. центра прибыли (1254121)', 'Наименование отдела (1254021)',
                                          'ТРП (120507)', 'Имя ТП (1205041)'],
                                 header=['Главкод', 'Наименование ГК', 'Код кл.', 'Название кл.', 'ИНН', 'Код сети',
                                         'БР', 'ЦП', 'ГП', 'ТРП', 'ФИО ТП']))
            print('Удалено строк: ', sum(mask))
            while True:  # проверка, если файл открыт
                try:
                    df[~mask].to_csv(data_export, sep=';', encoding='windows-1251', index=False, mode='w')
                    break
                except IOError:
                    input("Для изменения данных необходимо закрыть файл export_partners")
            print('Данные удалены из файла: ', data_export)
# Функции поиска
def main_partners(txt):    #поиск главкода по коду
    mask = df['Код клиента (120501)'] == txt
    main = df['Основной клиент (120510)'][mask]
    if len(main) > 0:
        mask = df['Основной клиент (120510)'] == main.values[0]
    else:
        print('карта партнера:', txt, 'отсутствует в базе')
        return df['Код клиента (120501)'] == '='  # заглушка для False
    return mask
def inn_partners(txt):    # поиск по ИНН
    mask = df['ИНН (120502)'] == txt
    return mask
def network_partners(txt):    # поиск по коду сети
    if txt[0] == 'net' or txt[0] == 'sety' or txt[0] == 'туе' or txt[0] == 'сети':
        mask = df['Код сети  (120535)'] != '0'
    else:
        mask = df['Код сети  (120535)'] == txt
    return mask
def trp_partners(txt):    # поиск по логину руководителя группы
    if len(txt) == 1 and mode == 'write' and (txt[0] == 'трп' or txt[0] == 'trp' or txt[0] == 'nhg'):
        mask = df['ТРП (120507)'] + ';' + df['Назв. Бизнес-рег. отд.тек.(1254221)'] + ';' + df['Наим. центра прибыли (1254121)'] + ';' + df['Наименование отдела (1254021)'] + ';' + df['Код Отдела (125402)']
        mask_unique = pd.DataFrame(mask.unique())    # создаем Серию из уникальных строк
        mask_unique = mask_unique[0].str.split(';', expand=True)
        mask_unique.columns = ['Логин ТРП', 'БР', 'ЦП', 'ГП', 'ЦФУ']
        print(mask_unique.to_string(index=False))
        while True:  # проверка, если файл открыт
            try:
                mask_unique.to_csv(data_export, sep=';', encoding='windows-1251', index=False)
                break
            except IOError:
                input("Для сохранения данных необходимо закрыть файл export_partners")
        print('Данные сохранены в файл: ', data_export)
        return df['Код клиента (120501)'] == '='    # заглушка для False
    elif len(txt) > 1 and (txt[0] == 'трп' or txt[0] == 'trp'):
        mask = df['Код клиента (120501)'] == '='    # заглушка для False
        for log_trp in txt:
            mask += df['ТРП (120507)'] == log_trp
        return mask
    else:
        print('Запрошенные данные в поле ТРП приведут к ошибке. Пожалуйста, сформулируйте запрос иначе.')
        return df['Код клиента (120501)'] == '='  # заглушка для False
def tp_partners(txt):    # поиск по ТП, формат ввода всегда должен начинаться с "ТП"
    if len(txt) == 1 and mode == 'write' and (txt[0] == 'тп' or txt[0] == 'tp' or txt[0] == 'ng'):    # выводит весь список уникальных ТП
        mask = df['Торговый представитель (120504)'] + ';' + df['Имя ТП (1205041)'] + ';' + df['Назв. Бизнес-рег. отд.тек.(1254221)'] + ';' + df['Наим. центра прибыли (1254121)'] + ';' + df['Наименование отдела (1254021)'] + ';' + df['Код Отдела (125402)']
        mask_unique = pd.DataFrame(mask.unique())    # создаем Серию из уникальных строк
        mask_unique = mask_unique[0].str.split(';', expand=True)
        mask_unique.columns = ['Код ТП', 'ФИО ТП', 'БР', 'ЦП', 'ГП', 'ЦФУ']
        mask_unique = mask_unique.sort_values(by=['БР', 'ЦП'])  # сортируем по БР
        print(mask_unique.to_string(index=False))
        while True:  # проверка, если файл открыт
            try:
                mask_unique.to_csv(data_export, sep=';', encoding='windows-1251', index=False)
                break
            except IOError:
                input("Для сохранения данных необходимо закрыть файл export_partners")
        print('Данные сохранены в файл: ', data_export)
        return df['Код клиента (120501)'] == '='  # заглушка для False
    elif len(txt) > 1 and txt[1].isdigit() and (txt[0] == 'тп' or txt[0] == 'tp'):    # поиск ТП по коду, можно несколько через пробел
        mask = df['Код клиента (120501)'] == '='    # заглушка False
        for log_tp in txt:    # перебираем все коды
            mask += df['Торговый представитель (120504)'] == log_tp
        return mask
    elif len(txt) > 1 and (txt[0] == 'тп' or txt[0] == 'tp'):
        txt = ' '.join(txt[1:]).split(',')    # пересобираем список: убираем ТП, соединяем ФИО с разделителем запятая
        df_tp = df['Имя ТП (1205041)'].str.lower()    # нижний регистр для поиска
        mask = df['Код клиента (120501)'] == '='    # заглушка False
        for name_tp in txt:   # цикл, если указано несколько ТР через запятую
            mask += df_tp.str.contains(name_tp.strip(), regex=False)   # удаляем лишние пробелы и собираем маску
        return mask
    else:
        print('Запрошенные данные в поле ТП приведут к ошибке. Пожалуйста, сформулируйте запрос иначе.')
        return df['Код клиента (120501)'] == '='  # заглушка для False
def cfu_partners(txt):    # Поиск по коду ЦФУ
    if len(txt) == 1 and mode == 'write' and (txt[0] == 'цфу' or txt[0] == 'cfu' or txt[0] == 'cfy' or txt[0] == 'wae'):    # выводит весь список уникальных ТП
        mask = df['Код Отдела (125402)'] + ';' + df['Назв. Бизнес-рег. отд.тек.(1254221)'] + ';' + df['Наим. центра прибыли (1254121)'] + ';' + df['Наименование отдела (1254021)'] + ';' + df['Назв. Бизнес-рег. отд.тек.(1254221)'] + '/' + df['Наим. центра прибыли (1254121)'] + '/' + df['Наименование отдела (1254021)']
        mask_unique = pd.DataFrame(mask.unique())    # создаем Серию из уникальных строк
        mask_unique = mask_unique[0].str.split(';', expand=True)
        mask_unique.columns = ['ЦФУ', 'БР', 'ЦП', 'ГП', 'Сцепка']
        mask_unique = mask_unique.sort_values(by=['БР', 'ЦП'])  # сортируем по БР
        print(mask_unique.to_string(index=False))
        while True:  # проверка, если файл открыт
            try:
                mask_unique.to_csv(data_export, sep=';', encoding='windows-1251', index=False)
                break
            except IOError:
                input("Для сохранения данных необходимо закрыть файл export_partners")
        print('Данные сохранены в файл: ', data_export)
        return df['Код клиента (120501)'] == '='  # заглушка для False
    elif len(txt) > 1 and (txt[0] == 'цфу' or txt[0] == 'cfu' or txt[0] == 'cfy'):
        mask = df['Код клиента (120501)'] == '='    # заглушка для False
        df_cfu = df['Код Отдела (125402)'].str.lower()  # нижний регистр для поиска
        for log_cfu in txt:
            mask += df_cfu == log_cfu
            print(sum(mask))
        print(df['Код Отдела (125402)'])
        return mask
    else:
        print('Запрошенные данные в поле ЦФУ приведут к ошибке. Пожалуйста, сформулируйте запрос иначе.')
        return df['Код клиента (120501)'] == '='  # заглушка для False
def vd_partners(txt):    # поиск по ВД
    if len(txt) == 1 and mode == 'write' and (txt[0] == 'вд' or txt[0] == 'vd' or txt[0] == 'dl'):  # выводит весь список уникальных ВД
        mask = df['Название основного в.д  (120523)'] + ';' + df['Название уточняющего в.д  (120525)']
        mask_unique = pd.DataFrame(mask.unique())  # создаем Серию из уникальных строк
        mask_unique = mask_unique[0].str.split(';', expand=True)    # разделяем столбцы уникальных значений
        mask_unique = mask_unique.dropna()    # удаляем пропуски
        mask_unique.columns = ['ВД', 'УВД']
        mask_unique = mask_unique.sort_values(by='ВД')  # сортируем по ВД
        print(mask_unique.to_string(index=False))
        while True:  # проверка, если файл открыт
            try:
                mask_unique.to_csv(data_export, sep=';', encoding='windows-1251', index=False)
                break
            except IOError:
                input("Для сохранения данных необходимо закрыть файл export_partners")
        print('Данные сохранены в файл: ', data_export)
        return df['Код клиента (120501)'] == '='  # заглушка для False
    elif len(txt) > 1 and (txt[0] == 'вд' or txt[0] == 'vd'):
        txt = ' '.join(txt[1:]).split(',') # пересобираем список: соединяем текст между запятыми, запятые используем как разделители
        df_vd = df['Название основного в.д  (120523)'].str.lower()    # нижний регистр для поиска
        mask = df['Код клиента (120501)'] == '='    # заглушка False
        for name_vd in txt:   # цикл, если указано несколько ТР через запятую
            mask += df_vd.str.contains(name_vd.strip(), regex=False)   # удаляем лишние пробелы и собираем маску
        return mask
    else:
        print('Запрошенные данные в поле ВД приведут к ошибке. Пожалуйста, сформулируйте запрос иначе.')
        return df['Код клиента (120501)'] == '='  # заглушка для False
def uvd_partners(txt):    # поиск по уточняющему ВД
    if len(txt) == 1 and mode == 'write' and (txt[0] == 'увд' or txt[0] == 'uvd' or txt[0] == 'yvd' or txt[0] == 'edl'):  # выводит весь список уникальных УВД
        mask = df['Название основного в.д  (120523)'] + ';' + df['Название уточняющего в.д  (120525)']
        mask_unique = pd.DataFrame(mask.unique())  # создаем Серию из уникальных строк
        mask_unique = mask_unique[0].str.split(';', expand=True)    # разделяем столбцы уникальных значений
        mask_unique = mask_unique.dropna()    # удаляем пропуски
        mask_unique.columns = ['ВД', 'УВД']
        mask_unique = mask_unique.sort_values(by='ВД')  # сортируем по ВД
        print(mask_unique.to_string(index=False))
        while True:  # проверка, если файл открыт
            try:
                mask_unique.to_csv(data_export, sep=';', encoding='windows-1251', index=False)
                break
            except IOError:
                input("Для сохранения данных необходимо закрыть файл export_partners")
        print('Данные сохранены в файл: ', data_export)
        return df['Код клиента (120501)'] == '='  # заглушка для False
    elif len(txt) > 1 and (txt[0] == 'увд' or txt[0] == 'uvd' or txt[0] == 'yvd'):
        txt = ' '.join(txt[1:]).split(',') # пересобираем список: соединяем текст между запятыми, запятые используем как разделители
        df_vd = df['Название уточняющего в.д  (120525)'].str.lower()    # нижний регистр для поиска
        mask = df['Код клиента (120501)'] == '='    # заглушка False
        for name_vd in txt:   # цикл, если указано несколько ТР через запятую
            mask += df_vd.str.contains(name_vd.strip(), regex=False)   # удаляем лишние пробелы и собираем маску
        return mask
    else:
        print('Запрошенные данные в поле УВД приведут к ошибке. Пожалуйста, сформулируйте запрос иначе.')
        return df['Код клиента (120501)'] == '='  # заглушка для False
def br_partners(txt):    # поиск по бизнес-региону
    if len(txt) == 1 and mode == 'write' and (txt[0] == 'бр' or txt[0] == 'br' or txt[0] == ',h'):  # выводит весь список уникальных БР
        mask = df['Назв. Бизнес-рег. отд.тек.(1254221)'] + ';' + df['Название региона отд.тек. (1254161)']
        mask_unique = pd.DataFrame(mask.unique())  # создаем Серию из уникальных строк
        mask_unique = mask_unique[0].str.split(';', expand=True)    # разделяем столбцы уникальных значений
        mask_unique = mask_unique.dropna()    # удаляем пропуски
        mask_unique.columns = ['БР', 'Регион']
        mask_unique = mask_unique.sort_values(by='БР')    # сортируем по БР
        print(mask_unique.to_string(index=False))
        while True:  # проверка, если файл открыт
            try:
                mask_unique.to_csv(data_export, sep=';', encoding='windows-1251', index=False)
                break
            except IOError:
                input("Для сохранения данных необходимо закрыть файл export_partners")
        print('Данные сохранены в файл: ', data_export)
        return df['Код клиента (120501)'] == '='  # заглушка для False
    elif len(txt) > 1 and (txt[0] == 'бр' or txt[0] == 'br'):
        txt = ' '.join(txt[1:]).split(',') # пересобираем список: соединяем текст между запятыми, запятые используем как разделители
        df_br = df['Назв. Бизнес-рег. отд.тек.(1254221)'].str.lower()    # нижний регистр для поиска
        mask = df['Код клиента (120501)'] == '='    # заглушка False
        for name_br in txt:   # цикл, если указано несколько ТР через запятую
            mask += df_br.str.contains(name_br.strip(), regex=False)   # удаляем лишние пробелы и собираем маску
        return mask
    else:
        print('Запрошенные данные в поле БР приведут к ошибке. Пожалуйста, сформулируйте запрос иначе.')
        return df['Код клиента (120501)'] == '='  # заглушка для False
def cp_partners(txt):    # поиск по центру прибыли
    if len(txt) == 1 and mode == 'write':  # выводит весь список уникальных БР
        mask = df['Наим. центра прибыли (1254121)'] + ';' + df['Код центра прибыли (125412)'] + ';' + df['Название региона отд.тек. (1254161)'] + ';' + df['Назв. Бизнес-рег. отд.тек.(1254221)']
        mask_unique = pd.DataFrame(mask.unique())  # создаем Серию из уникальных строк
        mask_unique = mask_unique[0].str.split(';', expand=True)    # разделяем столбцы уникальных значений
        mask_unique = mask_unique.dropna()    # удаляем пропуски
        mask_unique.columns = ['ЦП', 'Код ЦП', 'Регион', 'БР']
        mask_unique = mask_unique.sort_values(by=['БР', 'ЦП'])    # сортируем по БР
        print(mask_unique.to_string(index=False))
        while True:  # проверка, если файл открыт
            try:
                mask_unique.to_csv(data_export, sep=';', encoding='windows-1251', index=False)
                break
            except IOError:
                input("Для сохранения данных необходимо закрыть файл export_partners")
        print('Данные сохранены в файл: ', data_export)
        return df['Код клиента (120501)'] == '='  # заглушка для False
    elif len(txt) > 1:
        txt = ' '.join(txt[1:]).split(',') # пересобираем список: соединяем текст между запятыми, запятые используем как разделители
        df_br = df['Наим. центра прибыли (1254121)'].str.lower()    # нижний регистр для поиска
        mask = df['Код клиента (120501)'] == '='    # заглушка False
        for name_br in txt:   # цикл, если указано несколько ТР через запятую
            mask += df_br.str.contains(name_br.strip(), regex=False)   # удаляем лишние пробелы и собираем маску
        if sum(mask) == 0:    # если указан код ЦП, а не название, то продолжит поиск по коду ЦП
            df['Код центра прибыли (125412)'] = df['Код центра прибыли (125412)'].fillna('0')
            df['Код центра прибыли (125412)'] = df['Код центра прибыли (125412)'].astype('str')
            df_br = df['Код центра прибыли (125412)'].str.lower()  # нижний регистр для поиска
            for name_br in txt:  # цикл, если указано несколько ТР через запятую
                mask += df_br.str.contains(name_br.strip(), regex=False)  # удаляем лишние пробелы и собираем маску
        return mask
    else:
        print('Запрошенные данные в поле ЦП приведут к ошибке. Пожалуйста, сформулируйте запрос иначе.')
        return df['Код клиента (120501)'] == '='  # заглушка для False
def gp_partners(txt):    # поиск по группе продаж
    if len(txt) == 1 and mode == 'write' and (txt[0] == 'гп' or txt[0] == 'gp' or txt[0] == 'ug'):  # выводит весь список уникальных ГП
        mask = df['Наименование отдела (1254021)'] + ';' + df['Код Отдела (125402)'] + ';' + df['Наим. центра прибыли (1254121)'] + ';' + df['Код центра прибыли (125412)'] + ';' + df['Название региона отд.тек. (1254161)'] + ';' + df['Назв. Бизнес-рег. отд.тек.(1254221)']
        mask_unique = pd.DataFrame(mask.unique())  # создаем Серию из уникальных строк
        mask_unique = mask_unique[0].str.split(';', expand=True)    # разделяем столбцы уникальных значений
        mask_unique = mask_unique.dropna()    # удаляем пропуски
        mask_unique.columns = ['ГП', 'Код ЦФУ', 'ЦП', 'Код ЦП', 'Регион', 'БР']
        mask_unique = mask_unique.sort_values(by=['БР', 'ЦП', 'ГП'])    # сортируем по БР
        print(mask_unique.to_string(index=False))
        while True:  # проверка, если файл открыт
            try:
                mask_unique.to_csv(data_export, sep=';', encoding='windows-1251', index=False)
                break
            except IOError:
                input("Для сохранения данных необходимо закрыть файл export_partners")
        print('Данные сохранены в файл: ', data_export)
        return df['Код клиента (120501)'] == '='  # заглушка для False
    elif len(txt) > 1 and (txt[0] == 'гп' or txt[0] == 'gp' or txt[0] == 'ug'):
        txt = ' '.join(txt[1:]).split(',') # пересобираем список: соединяем текст между запятыми, запятые используем как разделители
        df_br = df['Наименование отдела (1254021)'].str.lower()    # нижний регистр для поиска
        mask = df['Код клиента (120501)'] == '='    # заглушка False
        for name_br in txt:   # цикл, если указано несколько ТР через запятую
            mask += df_br.str.contains(name_br.strip(), regex=False)   # удаляем лишние пробелы и собираем маску
        if sum(mask) == 0:    # если указан код ГП, а не название, то продолжит поиск по коду ЦФУ этой ГП
            df['Код Отдела (125402)'] = df['Код Отдела (125402)'].fillna('0')
            df['Код Отдела (125402)'] = df['Код Отдела (125402)'].astype('str')
            df_br = df['Код Отдела (125402)'].str.lower()  # нижний регистр для поиска
            for name_br in txt:  # цикл, если указано несколько ТР через запятую
                mask += df_br.str.contains(name_br.strip(), regex=False)  # удаляем лишние пробелы и собираем маску
        return mask
    else:
        print('Запрошенные данные в поле ГП приведут к ошибке. Пожалуйста, сформулируйте запрос иначе.')
        return df['Код клиента (120501)'] == '='  # заглушка для False
def name_partners(txt):    # поиск по наименованию партнера
    txt = ' '.join(txt).split(',') # пересобираем список: соединяем текст между запятыми, запятые используем как разделители
    df_name = df['Название клиента (12050102)'].str.lower()    # нижний регистр для поиска
    mask = df['Код клиента (120501)'] == '='    # заглушка False
    for name_txt in txt:   # цикл, если указано несколько наименований через запятую
        mask += df_name.str.contains(name_txt.strip(), regex=False)   # удаляем лишние пробелы и собираем маску
    if sum(mask) == 0:    # если нет результата поиска
        print('Партнеров с указанным наименованием не найдено')
    return mask
def sap_id_partners(txt):    #поиск по коду sap id
    mask = df['Код клиента (120501)'] == '='  # заглушка False
    if len(txt) > 1:
        for id in txt:
            mask += df['Код партнера в SAP (120551)'] == id
        return mask
    else:
        print('Значений не найдено. Проверьте формат ввода по SAP id: SAP 1883924 3870775')
        return df['Код клиента (120501)'] == '='  # заглушка для False
def street_partners(txt):    # поиск по ТП, формат ввода всегда должен начинаться с "ТП"
    if len(txt) > 1 and (txt[0] == 'ул.' or txt[0] == 'ул'or txt[0] == 'адрес'):
        txt = ' '.join(txt[1:]).split(',')    # пересобираем список: убираем ТП, соединяем ФИО с разделителем запятая
        df_tp = df['Фактический адрес (12050116)'].str.lower()    # нижний регистр для поиска
        mask = df['Код клиента (120501)'] == '='    # заглушка False
        for name_tp in txt:   # цикл, если указано несколько ТР через запятую
            mask += df_tp.str.contains(name_tp.strip(), regex=False)   # удаляем лишние пробелы и собираем маску
        if sum(mask) == 0:  # если нет результата поиска
            print('Адрес с указанным названием не выявлен')
        return mask
    else:
        print('Запрошенные данные в поле Адрес приведут к ошибке. Пожалуйста, сформулируйте запрос иначе.')
        return df['Код клиента (120501)'] == '='  # заглушка для False
def city_partners(txt):    # поиск по ТП, формат ввода всегда должен начинаться с "ТП"
    if len(txt) > 1 and (txt[0] == 'г' or txt[0] == 'г.'or txt[0] == 'город'):
        txt = ' '.join(txt[1:]).split(',')    # пересобираем список: убираем ТП, соединяем ФИО с разделителем запятая
        df_tp = df['Город факт.  (12050110)'].str.lower()    # нижний регистр для поиска
        mask = df['Код клиента (120501)'] == '='    # заглушка False
        for name_tp in txt:   # цикл, если указано несколько ТР через запятую
            mask += df_tp.str.contains(name_tp.strip(), regex=False)   # удаляем лишние пробелы и собираем маску
        if sum(mask) == 0:  # если нет результата поиска
            print('Город с указанным названием не выявлен')
        return mask
    else:
        print('Запрошенные данные в поле Адрес приведут к ошибке. Пожалуйста, сформулируйте запрос иначе.')
        return df['Код клиента (120501)'] == '='  # заглушка для False
def mail_partners(txt):    # поиск по ТП, формат ввода всегда должен начинаться с "ТП"
    if len(txt) > 1 and (txt[0] == '@' or txt[0] == 'e-mail:'):
        txt = ' '.join(txt[1:]).split(',')    # пересобираем список: убираем ТП, соединяем ФИО с разделителем запятая
        df_tp = df['Эл. почта  (12540112)'].str.lower()    # нижний регистр для поиска
        mask = df['Код клиента (120501)'] == '='    # заглушка False
        for name_tp in txt:   # цикл, если указано несколько ТР через запятую
            mask += df_tp.str.contains(name_tp.strip(), regex=False)   # удаляем лишние пробелы и собираем маску
        if sum(mask) == 0:  # если нет результата поиска
            print('Почта с указанным названием не выявлена')
        return mask
    else:
        print('Запрошенные данные в поле Адрес приведут к ошибке. Пожалуйста, сформулируйте запрос иначе.')
        return df['Код клиента (120501)'] == '='  # заглушка для False
def unique_main_partners(txt):    # оставить только главкоды
    mask = df['Код клиента (120501)'] == df['Основной клиент (120510)']
    return mask
def about_program(txt):
    print('-' * 30)
    print('Программа "Smart partners base"    # by Kirill Kasparov, 2022.')
    print('Описание: программа работает как фильтр поиска по партнерской базе. Отличительной особенностью является система умного поиска, позволяющая определить тип запроса и предоставить максимально полную информацию по партнеру. Результат поиска сохраняется в файле export_partners.csv в той же папке, где расположена программа. Во время поиска, файл export_partners.csv должен быть закрыт.')
    print('Путь к файлу, где сохранен результат Вашего поиска на компьютере: ', data_export)
    print('-' * 30)
    print('Реализованы механизмы поиска:')
    print('По коду партнера. Пример запроса: 2830207 1670771 1069153')
    print('По наименованию партнера. Пример запроса: Газпром, Торгсервис')
    print('По ИНН. Пример запроса: 7706564354 7702836198')
    print('По коду сети. Ключ «Сети» отобразит базу всех сетевых. Пример запроса: 100 227 2830')
    print('«SAP» - по коду SAP id. Пример запроса: сап 3870775 1906719')
    print('«ТРП» - по руководителю группы. Пример запроса: ТРП mma12 aen28 baeeq ')
    print('«ТП» - по торговому представителю. Пример запроса: ТП Гутников Алексей, Коновалов, Дмитрий Владимирович')
    print('«ЦФУ» - По коду ЦФУ. Пример запроса: ЦФУ 1QE 23L 24H')
    print('«ВД» - по виду деятельности. Пример запроса: ВД Торговля, Образование')
    print('«УВД» - по уточняющему виду деятельности. Пример запроса: УВД Стоматология, Клининг')
    print('«БР» - по бизнес-региону. Пример запроса: БР Урал, Сибирь')
    print('«ЦП» - по центру прибыли. Пример запроса: ЦП Торговля, Офис')
    print('«ГП» - по названию группы продаж. Пример запроса: ГП Торговля, Производство')
    print('-' * 30)
    print('Уточняющие запросы к уже сформированным данным.')
    print('+ добавить информацию к сформированной базе. Пример запроса: + ЦФУ 1QE')
    print('- удалить информация из сформированной базы. Пример запроса: - БР Москва, ЦФО')
    print('= оставляет только уазанные значения. Пример запроса: = ТП 5236')
    print('= Главкод - оставить только строки с главкодом, исключив подглавкодные карты.')
    print('0 - выход из программы.')
    print('-' * 30)

    return df['Код клиента (120501)'] == '='  # заглушка для False
def updade_base(txt):
    print('Последняя вервия базы: ', new_base)
    print('Загружаем обновление, это займет несколько минут...')
    df = pd.read_excel(new_base, engine='pyxlsb')
    print('Сохраняем базу на диске...')
    df.to_csv(data_import, sep=';', encoding='windows-1251', index=False, mode='w')
    if os.path.exists("\\\TU-AO-FS-1/UserArea/Каспаров/Программы/smart_partners_base/"):
        if os.path.getctime(data_import) > os.path.getctime('\\\TU-AO-FS-1/UserArea/Каспаров/Программы/smart_partners_base/import.csv'):
            import shutil
            os.remove('\\\TU-AO-FS-1/UserArea/Каспаров/Программы/smart_partners_base/import.csv')
            shutil.copyfile(data_import, '\\\TU-AO-FS-1/UserArea/Каспаров/Программы/smart_partners_base/import.csv')
    cleaner_base()
    print('Обновление завершено.')
    return df['Код клиента (120501)'] == '='  # заглушка для False
def cleaner_base():    # Чистим столбцы
    print('Настраиваем форматы столбцов...')
    df['Код клиента (120501)'] = df['Код клиента (120501)'].fillna('0')
    df['Код клиента (120501)'] = df['Код клиента (120501)'].astype('str')
    df['Код клиента (120501)'] = df['Код клиента (120501)'].str.replace('.0', '', regex=False)
    df['Основной клиент (120510)'] = df['Основной клиент (120510)'].fillna('0')
    df['Основной клиент (120510)'] = df['Основной клиент (120510)'].astype('str')
    df['Основной клиент (120510)'] = df['Основной клиент (120510)'].str.replace('.0', '', regex=False)
    df['ИНН (120502)'] = df['ИНН (120502)'].fillna('0')
    df['ИНН (120502)'] = df['ИНН (120502)'].astype('str')
    df['ИНН (120502)'] = df['ИНН (120502)'].str.replace('.0', '', regex=False)
    df['Код сети  (120535)'] = df['Код сети  (120535)'].fillna('0')
    df['Код сети  (120535)'] = df['Код сети  (120535)'].astype('str')
    df['Код сети  (120535)'] = df['Код сети  (120535)'].str.replace(' ', '')
    df['Код сети  (120535)'] = df['Код сети  (120535)'].str.replace(',00', '')
    df['Торговый представитель (120504)'] = df['Торговый представитель (120504)'].fillna(0)
    df['Торговый представитель (120504)'] = df['Торговый представитель (120504)'].astype('str')
    df['Торговый представитель (120504)'] = df['Торговый представитель (120504)'].str.replace('.0', '', regex=False)
    df['Имя ТП (1205041)'] = df['Имя ТП (1205041)'].fillna(0)
    df['Имя ТП (1205041)'] = df['Имя ТП (1205041)'].astype('str')
    df['Название основного в.д  (120523)'] = df['Название основного в.д  (120523)'].fillna('0')
    df['Название основного в.д  (120523)'] = df['Название основного в.д  (120523)'].astype('str')
    df['Название уточняющего в.д  (120525)'] = df['Название уточняющего в.д  (120525)'].fillna('0')
    df['Название уточняющего в.д  (120525)'] = df['Название уточняющего в.д  (120525)'].astype('str')
    df['Назв. Бизнес-рег. отд.тек.(1254221)'] = df['Назв. Бизнес-рег. отд.тек.(1254221)'].fillna('0')
    df['Назв. Бизнес-рег. отд.тек.(1254221)'] = df['Назв. Бизнес-рег. отд.тек.(1254221)'].astype('str')
    df['Наим. центра прибыли (1254121)'] = df['Наим. центра прибыли (1254121)'].fillna('0')
    df['Наим. центра прибыли (1254121)'] = df['Наим. центра прибыли (1254121)'].astype('str')
    df['Наименование отдела (1254021)'] = df['Наименование отдела (1254021)'].fillna('0')
    df['Наименование отдела (1254021)'] = df['Наименование отдела (1254021)'].astype('str')
    df['Название клиента (12050102)'] = df['Название клиента (12050102)'].fillna('0')
    df['Название клиента (12050102)'] = df['Название клиента (12050102)'].astype('str')
    df['Код партнера в SAP (120551)'] = df['Код партнера в SAP (120551)'].fillna('0')
    df['Код партнера в SAP (120551)'] = df['Код партнера в SAP (120551)'].astype('str')
    df['Код партнера в SAP (120551)'] = df['Код партнера в SAP (120551)'].str.replace('.0', '', regex=False)
    df['Фактический адрес (12050116)'] = df['Фактический адрес (12050116)'].fillna('0')
    df['Фактический адрес (12050116)'] = df['Фактический адрес (12050116)'].astype('str')
    df['Город факт.  (12050110)'] = df['Город факт.  (12050110)'].fillna('0')
    df['Город факт.  (12050110)'] = df['Город факт.  (12050110)'].astype('str')
    df['Эл. почта  (12540112)'] = df['Эл. почта  (12540112)'].fillna('0')
    df['Эл. почта  (12540112)'] = df['Эл. почта  (12540112)'].astype('str')

# Тело кода
cmd = 'mode 180,30'
os.system(cmd)

# определяем абсолютный путь к файлам
data_import = os.getcwd().replace('\\', '/') + '/' + 'import.csv'
data_export = os.getcwd().replace('\\', '/') + '/' + 'export_partners.csv'
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
# логи
if os.path.exists('\\\TU-AO-FS-1/UserArea/Каспаров/Программы/smart_partners_base/'):
    import datetime
    user = pd.Series(os.getcwd().split('\\')[2] + ';' + str(datetime.date.today()))
    user.to_csv('\\\TU-AO-FS-1/UserArea/Каспаров/Программы/smart_partners_base/users.csv', sep=';', encoding='windows-1251', index=False, mode='a', header=False)


# Запускаем цикл поиска - повторять до команды выхода
while txt_input != '0':    # '0' - выход
    print('-' * 30)
    while True:  # проверка, если файл открыт
        try:
            txt_input = input('Введите запрос: ').lower()
            if len(txt_input) != 0:
                break
        except KeyboardInterrupt:
            print("Такой тип ввода приведет к ошибке программы. Попробуйте скопировать данные еще раз, без переносов строк.")
    txt_list = txt_input.split()    # проверяем список


    func1 = save_mode_selection(txt_list)    # функция выбора режима сохранения файла
    txt_list, mode = func1[0], func1[1]
    if mode == 'equal' or mode == 'delete':
        print('Загружаем раннее сформированную базу данных...')
        while True:  # проверка, если файл открыт
            try:
                df2 = pd.read_csv(data_export, sep=';', encoding='windows-1251', low_memory=False, nrows=1000000)  # загружаем базу
                break
            except IOError:
                input("Для загрузки данных необходимо закрыть файл export_partners")
        df, df2 = df2, df
        print('База данных загружена, обрабатываем запрос.')
    else:
        df2 = False    # убрать ошибку

    mask = df['Код клиента (120501)'] == '='  # сводная маска для нескольких значений
    count_txt_list = 1    # счетчик, чтобы веселее было ждать перебор нескольких значений
    if txt_list[0].replace(',', '').isdigit():    # если первое значение = цифра, передаем по одному и записываем в общую маску
        for txt in txt_list:
            if len(txt_list) > 1:
                print('Введено значений: ', len(txt_list), 'Обработано: ', count_txt_list)
                count_txt_list += 1
            if txt.isdigit():
                mask += filter_txt(txt.replace(',', ''))
            else:
                print('В переданный список попало нечисловое значение. Программа не работает со смешанным типом данных в одном запросе.')
    else:    # во всех остальных случаях передаем весь список, а не один элемент
        mask += filter_txt(txt_list)

    if txt_input != '0':    # только для возвращенных масок. Исключает уникальные списки-справочники ЦФУ/БР/ГП/ТРП/ТП
        if sum(mask) != 0:
            save_mode(mode)
            if mode == 'equal' or mode == 'delete':
                df, df2 = df2, df
    else:
        print('Bye-bye!')
