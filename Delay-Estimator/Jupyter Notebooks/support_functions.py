import pandas as pd
import datetime as dt
import re


# lista_in é um elemento da rdd
def trata_rdd(linha_in):
    lista_in = linha_in.split('|')
    
    # cria lista
    lista_out = list()
    
    # transforma os dois primeiros elementos em inteiro 
    lista_out.append(int(lista_in[0]))
    lista_out.append(int(lista_in[1]))
    
    # trata as listas seguintes, eliminando [,],', e espaço, separando com ',':
    lista_out.append(re.sub('\[|\]| |\'', '', lista_in[2]).split(','))
    lista_out.append([int(x) for x in re.sub(r'\[|\]| |\'', '', lista_in[3]).split(',')])
    lista_out.append([float(x) for x in re.sub(r'\[|\]| |\'', '', lista_in[4]).split(',')])
    
    # retorna a lista resultante
    return lista_out

# split file
def split_file(input_file, output_path = '.', file_len = 100000):

    dest_files = list()
    dest_filenames = list()

    file_num = 1

    with open(input_file, 'r') as source_f:
        for l, line in enumerate(source_f):
            if not l % file_len:
                dest_filename = output_path + '/sales_data_part_' + str(file_num) + '.csv'

                dest_files.append((dest_filename, open(dest_filename, 'w')))

                file_num += 1
                print('Writing file ' + dest_files[-1][0], end = '\r')

            dest_files[-1][1].write(line)

    for name, dest_file in dest_files:
        dest_file.close()
        dest_filenames.append(name)
        print('Closing file ' + name, end = '\r')

    print('\nFinished!')
    return dest_filenames

# get_top_5 para aulas 01 e 02
import re

def get_top5(file, nrows = None):
    
    if nrows == None:
        with open(file) as f:
            lines = f.readlines()
    else:
        lines = list()
        with open(file) as f:
            for i in range(0, nrows):
                lines.append(f.readline())
          
    splitted_lines = map(lambda l: l.split('|'), lines)
    
    converted_lines_01 = map(lambda el: [int(el[0]), \
                       int(el[1]), \
                       re.findall('\d{4}-\d{2}-\d{2}', el[2]), \
                       re.findall('\d+\.*\d*', el[3]), \
                       re.findall('\d+\.*\d*', el[4])], splitted_lines)
    
    converted_lines_02 = map(lambda el: el[0:3] + \
                       [[int(i) for i in el[3]]] + \
                       [[float(i) for i in el[4]]], converted_lines_01)
    
    converted_lines = converted_lines_02
    
    unit_price_lines = map(lambda el: el + [[p / q for p, q in zip(el[4], el[3])]], converted_lines)
    
    max_price_lines = map(lambda el: el + [max(el[-1])], unit_price_lines)
    
    top5 = list(sorted(max_price_lines, key = lambda el: -el[-1]))[0:5]
    
    return top5

def print_top5(top5, n = 5):
    if n > 5:
        n = 5
        
    for i, el in enumerate(top5[:n]):
        idx = el[-2].index(el[-1])
        print('TOP {}\n'.format(i+1) + 
              '  Product ID: {}'.format(el[0]) + 
              '   | Store ID: {}\n'.format(el[1]) + 
              '  Date: {}'.format(el[2][idx]) +
              ' | Quantity: {}\n'.format(el[3][idx]) + 
              '  Price Total: {}'.format(el[4][idx]) +
              ' | Unit: {}\n'.format(el[5][idx])) 

def flatten_list(list_in):
    return [item for sublist in list_in for item in sublist]

def rdd2rel(rdd):
    rdd2 = rdd.keyBy(lambda row: (row[0], row[1]))

    def row2tuple(row):
        return [(d,p,q) for d,p,q in zip(row[2],row[3],row[4])]

    rdd3 = rdd2.flatMapValues(row2tuple)

    rdd3.take(3)

    rdd4 = rdd3.map(lambda row: [el for el in row[0]] + [el for el in row[1]])
    
    return rdd4

def set_spark_python():
    import os

    os.environ['PYSPARK_PYTHON'] = '/anaconda3/envs/spark/bin/python'
    os.environ['PYSPARK_DRIVER_PYTHON'] = '/anaconda3/envs/spark/bin/python'
    