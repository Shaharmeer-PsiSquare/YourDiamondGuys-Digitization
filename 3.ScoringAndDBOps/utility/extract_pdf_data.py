import decimal
import json
import logging
import re
from itertools import takewhile
from pathlib import Path
from typing import TypedDict, Optional

import cv2
import fitz
import numpy as np
import requests


pattern_digi = r'\d+[,.]?\d+'

logger = logging.getLogger(__name__)


def take_second(tup):
    return tup[0][0][0][1]


_BASE_DIR = Path(__file__).parent
_DIAMONDS_CONFIG_PATH = _BASE_DIR / "diamonds_type_config.json"
_CHAR_CONFIG_PATH = _BASE_DIR / "config.json"

with _DIAMONDS_CONFIG_PATH.open("r", encoding="utf-8") as fs:
    data = json.load(fs)
    logger.info(
        "Loaded %d diamond type configuration entries from %s",
        len(data),
        _DIAMONDS_CONFIG_PATH,
    )

with _CHAR_CONFIG_PATH.open("r", encoding="utf-8") as f:
    characteristic_data = json.load(f)
    logger.info(
        "Loaded %d characteristic configuration entries from %s",
        len(characteristic_data),
        _CHAR_CONFIG_PATH,
    )



def fetch_data_from_gia(text, refine_dic, round_fg):
    # GIA Report Number
    report_number = re.search(r'GIA Report Number\s*\.*\s*(\d+)', text).group(1)

    # Shape and Cutting Style
    shape_cutting_style = re.search(r'Shape and Cutting Style\s*\.*\s*([A-Za-z\s]+)', text).group(1)
    shape_cutting_style_ls = shape_cutting_style.split('\n')
    shape_cutting_style = shape_cutting_style_ls[0] if len(shape_cutting_style_ls) >= 2 else shape_cutting_style
    # Measurements
    if round_fg:
        measurements = re.search(r'Measurements\s*\.*\s*([\d.]+)\s*-\s*([\d.]+)\s*x\s*([\d.]+)', text)
        measurement_min = measurements.group(1)
        measurement_max = measurements.group(2)
        measurement_depth = measurements.group(3)
    else:
        measurements = re.search(r'Measurements\s*\.*\s*([\d.]+)\s*x\s*([\d.]+)\s*x\s*([\d.]+)', text)
        measurement_min = measurements.group(1)
        measurement_max = measurements.group(2)
        measurement_depth = measurements.group(3)

    # Carat Weight
    carat_weight = re.search(r'Carat Weight\s*\.*\s*([\d.]+)\s*carat', text).group(1)

    # Color Grade
    color_grade = re.search(r'Color Grade\s*\.*\s*([A-Za-z\s]+)', text).group(1)
    color_grade_ls = color_grade.split('\n')
    color_grade = color_grade_ls[0] if len(color_grade_ls) >= 2 else color_grade

    # Clarity Grade
    clarity_grade = re.search(r'Clarity Grade\s*\.*\s*([A-Za-z0-9\s]+)', text).group(1)
    clarity_grade_ls = clarity_grade.split('\n')
    clarity_grade = clarity_grade_ls[0] if len(clarity_grade_ls) >= 2 else clarity_grade
    # Cut Grade
    if round_fg:
        cut_grade = re.search(r'Cut Grade\s*\.*\s*([A-Za-z\s]+)', text).group(1)
        cut_grade_ls = cut_grade.split('\n')
        if len(cut_grade_ls) == 2:
            cut_grade = cut_grade_ls[0]
        refine_dic["cut"] = cut_grade

    # polish
    polish = re.search(r'Polish\s*\.*\s*([A-Za-z\s]+)', text, re.DOTALL).group(1)
    polish_ls = polish.split('\n')
    if len(polish_ls) == 2:
        polish = polish_ls[0]

    # Symmetry
    symmetry = re.search(r'Symmetry\s*\.*\s*([A-Za-z\s]+)', text, re.DOTALL).group(1)
    symmetry_ls = symmetry.split('\n')
    symmetry = symmetry_ls[0] if len(symmetry_ls) >= 2 else symmetry

    # Fluorescence
    fluorescence = re.search(r'Fluorescence\s*\.*\s*([A-Za-z\s]+)', text).group(1)
    fluorescence_ls = fluorescence.split('\n')
    fluorescence = ','.join(fluorescence_ls[:-1]) if fluorescence_ls[-1] == 'Clarity Characteristics ' or \
                                                     fluorescence_ls[-1] == 'Clarity Characteristics' or \
                                                     fluorescence_ls[-1] == 'Inscription' else ','.join(fluorescence_ls)
    try:
        # Clarity Characteristics
        clarity_characteristics = re.search(r'Clarity Characteristics\s*\.*\s*([\w\s,]+)', text).group(1)
        clarity_characteristics_ls = clarity_characteristics.split('\n')
        clarity_characteristics = ','.join(clarity_characteristics_ls[:-1]) if clarity_characteristics_ls[
                                                                                   -1].lower().strip() == 'inscription' else ','.join(
            clarity_characteristics_ls)
    except:
        pattern = r'KEY TO SYMBOLS\*[\s\n]*(.+?)(?=\s*\*)'
        key_to_symbols_match = re.search(pattern, text, re.DOTALL)

        if key_to_symbols_match:
            key_to_symbols = key_to_symbols_match.group(1).strip()
            elements_list = key_to_symbols.split('\n')
            condition = lambda \
                    x: x != "* Red symbols denote internal characteristics (inclusions). Green or black symbols denote external characteristics"
            # Use takewhile to fetch elements before "* Red"
            filtered_elements = list(takewhile(condition, elements_list))
            clarity_characteristics = filtered_elements
        else:
            clarity_characteristics = "Key to symbols not found in the data."
        clarity_characteristics = ",".join(clarity_characteristics)

    refine_dic["certificate_id"] = report_number
    refine_dic["shape"] = shape_cutting_style
    if round_fg:
        refine_dic["measurement"] = measurement_min + "-" + measurement_max + "x" + measurement_depth
    else:
        refine_dic["measurement"] = measurement_min + "x" + measurement_max + "x" + measurement_depth
    refine_dic["carat"] = carat_weight
    refine_dic["color_grade"] = color_grade
    refine_dic["clarity"] = clarity_grade
    refine_dic["polish"] = polish
    refine_dic["symmetry"] = symmetry
    refine_dic["fluorescence"] = fluorescence
    refine_dic["key_to_symbol"] = clarity_characteristics

    return refine_dic


def check_fluorescence(flour_value, characteristic_json, Color_Grade):
    flour_value_ls = flour_value.split()
    if len(flour_value_ls) == 2 and flour_value_ls[0].lower() != 'very':
        data_f = flour_value
    else:
        if flour_value.lower() == 'none':
            data_f = 'default'
        else:
            data_f = flour_value
    info = None
    for data in characteristic_json:
        if data['characteristic_name'] == "FLUORESCENCE":
            info = data['characteristic_data']
            break
    for f in info:
        if Color_Grade.rstrip() == f['data_type']:
            very_strong_value = next(
                filter(lambda config: config["name"].lower() == data_f.lower(), f['type_configuration']), {}).get(
                "value")
            return very_strong_value

    return None


def check_girdle(threshold, value, characteristic_json):
    info = None
    for data in characteristic_json:
        if data['characteristic_name'] == "GIRDLE THICKNESS":
            info = data['characteristic_data']
            break
    if value in threshold and threshold is not 'None':
        for i in info:
            if i['name'].lower() == value.lower():
                point = i["value"]
                return point
    else:
        for i in info:
            if i['name'].lower() == value.lower():
                point = i["value"]
                return point
    return 0


def check_polish(threshold="None", value=None, characteristic_json=None):
    info = None
    for data in characteristic_json:
        if data['characteristic_name'] == "ROUND SCORE":
            info = data['characteristic_data']
            break
    if value.lower() in threshold and threshold != 'None':
        for i in info:
            if i['data_type'].lower().strip() == value.lower().strip():
                config = i['type_configuration'][1]
                point = config['value']
                return point
    else:
        for i in info:
            if i['data_type'].lower() == value.lower():
                config = i['type_configuration'][1]
                point = config['value']
                return point
    return 0


def check_symmetry(threshold, value, characteristic_json):
    info = None
    for data in characteristic_json:
        if data['characteristic_name'] == "ROUND SCORE":
            info = data['characteristic_data']
            break
    if value.lower() in threshold and threshold != 'None':
        for i in info:
            if i['data_type'].lower() == value.lower():
                config = i['type_configuration'][2]
                point = config['value']
                return point
    else:
        for i in info:
            if i['data_type'].lower() == value.lower():
                config = i['type_configuration'][2]
                point = config['value']
                return point
    return 0


def check_symbol(data_type_to_fetch, data_con):
    data_type_to_fetch = data_type_to_fetch.upper().replace("TWINNING WISP", "TWINNINGWISP")
    data_type_to_fetch = data_type_to_fetch.upper().replace("INDENTED NATURAL", "INDENTEDNATURAL")
    data_type_to_fetch = data_type_to_fetch.upper().replace("GROWTH REMNANT", "GROWTHREMNANT")
    if isinstance(data_type_to_fetch, list):
        data_ls = data_type_to_fetch
    else:
        if ',' in data_type_to_fetch:
            data_ls = data_type_to_fetch.split(',')
        else:
            # if
            data_ls = data_type_to_fetch.split()

    info = None
    for data in data_con:
        if data['characteristic_name'] == "KEYS TO SYMBOLS":
            info = data['characteristic_data']
            break
    if len(data_ls) == 1:
        data_type_to_fetch = data_type_to_fetch.upper()
        if data_type_to_fetch == "TWINNINGWISP":
            data_type_to_fetch = "TWINNING WISP"
        elif data_type_to_fetch == "INDENTEDNATURAL":
            data_type_to_fetch = "INDENTED NATURAL"

        pinpoint_data = next(filter(lambda data: data["data_type"] == data_type_to_fetch, info), None)
        if pinpoint_data is not None:
            sum_point = pinpoint_data['type_configuration'][0]['value']
            return sum_point
        else:
            return 5
        # sum_point = pinpoint_data['type_configuration'][0]['value']
    else:

        score = []
        next_idx = False
        ind_flag = False
        for id, d in enumerate(data_ls):
            try:
                if d.lower().strip() == "indented" and data_ls[id + 1].lower().strip() == 'natural':
                    ind_flag = True
                    # next_idx = True
                    d = 'Indented Natural'
                elif d.lower().strip() == "indented":
                    d = 'Indented Natural'
            except:
                if d.lower().strip() == "indented":
                    d = 'Indented Natural'
            if d != '' and ind_flag is False and next_idx is False:

                data_type_to_fetch = d.upper().strip().replace(" ", '')

                if data_type_to_fetch == "TWINNINGWISP":
                    data_type_to_fetch = "TWINNING WISP"
                elif data_type_to_fetch == "INDENTEDNATURAL":
                    data_type_to_fetch = "INDENTED NATURAL"

                pinpoint_data = next(filter(lambda data: data["data_type"] == data_type_to_fetch, info), None)
                # print(pinpoint_data)
                if pinpoint_data is not None:
                    score.append(pinpoint_data['type_configuration'][0]['value'])
                else:
                    pass

            elif ind_flag is True:
                # ind_flag = False
                data_type_to_fetch = d.upper().strip()

                if data_type_to_fetch == "TWINNINGWISP":
                    data_type_to_fetch = "TWINNING WISP"

                pinpoint_data = next(filter(lambda data: data["data_type"] == data_type_to_fetch, info), None)
                if pinpoint_data is not None:
                    score.append(pinpoint_data['type_configuration'][0]['value'])
                else:
                    pass
            elif next_idx:
                ind_flag = False
                next_idx = False
        if score:
            return min(score)
        else:
            return 5


color_grade_ls = ["D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W",
                  "X", "Y", "Z"]


def fetch_data(req_dc, pdf_dc, affiliate_process_data, shape=None):
    logger.debug(
        "Starting non-round fetch_data for shape=%s with affiliate_process_data=%s",
        pdf_dc.get("shape"),
        bool(affiliate_process_data),
    )
    pdf_dc_new = pdf_dc.copy()
    # print("affiliate_process_data::", affiliate_process_data)
    if affiliate_process_data is not None and affiliate_process_data:
        if affiliate_process_data["shape"] is not None and affiliate_process_data["shape"] != '' and \
                affiliate_process_data["shape"] != 'null' and affiliate_process_data["shape"] != 'Null' and \
                affiliate_process_data["shape"] != 'False' and affiliate_process_data["shape"] != 'false':
            pdf_dc['shape'] = affiliate_process_data["shape"]
        if affiliate_process_data["carat"] is not None and affiliate_process_data["carat"] != 'false' and \
                affiliate_process_data["carat"] != '' and affiliate_process_data["carat"] != 'none' and \
                affiliate_process_data["carat"] != 'None' and affiliate_process_data["carat"] != 'null' and \
                affiliate_process_data["carat"] != 'Null':
            pdf_dc['carat'] = affiliate_process_data["carat"]
        if affiliate_process_data["table_size"] is not None and affiliate_process_data["table_size"] != 'Null' and \
                affiliate_process_data["table_size"] != 'none' and affiliate_process_data["table_size"] != '' and \
                affiliate_process_data["table_size"] != 'None' and affiliate_process_data["table_size"] != 'false' and \
                affiliate_process_data["table_size"] != 'False':
            pdf_dc['table_size'] = affiliate_process_data["table_size"]
        if affiliate_process_data["griddle"] is not None and affiliate_process_data["griddle"] != 'false' and \
                affiliate_process_data["griddle"] != 'False' and affiliate_process_data["griddle"] != '' and \
                affiliate_process_data["griddle"] != 'none' and affiliate_process_data["griddle"] != 'None' and \
                affiliate_process_data["griddle"] != 'null' and affiliate_process_data["griddle"] != 'Null':
            pdf_dc['girdle'] = affiliate_process_data["griddle"]
        if affiliate_process_data["depth"] is not None and affiliate_process_data["depth"] != 'null' and \
                affiliate_process_data["depth"] != 'Null' and affiliate_process_data[
            "depth"] != 'false' and affiliate_process_data["depth"] != 'False' and affiliate_process_data[
            "depth"] != 'none' and affiliate_process_data["depth"] != 'None' and affiliate_process_data["depth"] != '':
            pdf_dc['depth'] = affiliate_process_data["depth"]
        if affiliate_process_data["measurement"] is not None and affiliate_process_data["measurement"] != 'False' and \
                affiliate_process_data["measurement"] != 'false' and affiliate_process_data["measurement"] != '' and \
                affiliate_process_data["measurement"] != 'null' and affiliate_process_data["measurement"] != 'Null' and \
                affiliate_process_data["measurement"] != 'none' and affiliate_process_data["measurement"] != 'None':
            pdf_dc['measurement'] = affiliate_process_data["measurement"]
        if affiliate_process_data["culet"] is not None and affiliate_process_data["culet"] != '' and \
                affiliate_process_data["culet"] != 'false' and affiliate_process_data["culet"] != 'False' and \
                affiliate_process_data["culet"] != 'null' and affiliate_process_data["culet"] != 'Null':
            pdf_dc['culet'] = affiliate_process_data["culet"]

        if affiliate_process_data["color"] is not None and affiliate_process_data["color"] != 'none' and \
                affiliate_process_data["color"] != 'None' and affiliate_process_data["color"] != '' and \
                affiliate_process_data["color"] != 'False' and affiliate_process_data["color"] != 'false' and \
                affiliate_process_data["color"] != 'null' and affiliate_process_data["color"] != 'Null':
            pdf_dc['color_grade'] = affiliate_process_data["color"]

        if affiliate_process_data["polish"] is not None and affiliate_process_data["polish"] != 'false' and \
                affiliate_process_data["polish"] != 'False' and affiliate_process_data["polish"] != '' and \
                affiliate_process_data["polish"] != 'None' and affiliate_process_data["polish"] != 'none' and \
                affiliate_process_data["polish"] != 'null' and affiliate_process_data["polish"] != 'Null':
            pdf_dc['polish'] = affiliate_process_data["polish"]

        if affiliate_process_data["symmetry"] is not None and affiliate_process_data["symmetry"] != '' and \
                affiliate_process_data["symmetry"] != 'false' and affiliate_process_data["symmetry"] != 'False' and \
                affiliate_process_data["symmetry"] != 'none' and affiliate_process_data["symmetry"] != 'None' and \
                affiliate_process_data["symmetry"] != 'null' and affiliate_process_data["symmetry"] != 'Null':
            pdf_dc['symmetry'] = affiliate_process_data["symmetry"]
        if affiliate_process_data["fluorescence"] is not None and affiliate_process_data["fluorescence"] != '' and \
                affiliate_process_data["fluorescence"] != 'False' and affiliate_process_data[
            "fluorescence"] != 'false' and affiliate_process_data["fluorescence"] != 'null' and affiliate_process_data[
            "fluorescence"] != 'Null':
            pdf_dc['fluorescence'] = affiliate_process_data["fluorescence"]
        if affiliate_process_data["cut"] is not None and affiliate_process_data["cut"] != '' and affiliate_process_data[
            "cut"] != 'none' and affiliate_process_data["cut"] != 'None' and affiliate_process_data[
            "cut"] != 'false' and affiliate_process_data["cut"] != 'False' and affiliate_process_data[
            "cut"] != 'null' and affiliate_process_data["cut"] != 'Null':
            pdf_dc['cut'] = affiliate_process_data["cut"]

    type = None
    if "shape" in pdf_dc:
        if "shape" in pdf_dc:
            type = pdf_dc['shape'].split()
            if "Square Emerald" in pdf_dc['shape']:
                type[0] = "asscher"
            elif "Square Modified" in pdf_dc['shape']:
                type[0] = "princess"
            # elif "Cut Cornered Rectangular" in pdf_dc['shape']:
            #     type[0] = "radiant rec"
            shape_str = pdf_dc['shape'].upper()  # make case-insensitive
            if "CUT CORNERED RECTANGULAR" in shape_str:
                type[0] = "radiant rec"
            if "CUT EMERALD" in pdf_dc['shape']:
                type[0] = "emerald"
            elif "Cut-Cornered" in pdf_dc['shape'] and "Rectangular" in pdf_dc['shape']:
                type[0] = "radiant rec"
            elif "Cut-Cornered " in pdf_dc['shape'] and "square" in pdf_dc['shape']:
                type[0] = "radiant sq"
            elif type[0].lower() == 'cc':
                type[0] = "cushion"
            elif 'cushion' in pdf_dc['shape'].lower():
                pdf_dc['shape'] = "cushion"
                type[0] = "cushion"
            elif "Cut-Cornered " in pdf_dc['shape'] or "Rectangular" in pdf_dc['shape']:
                type[0] = "radiant rec"
            elif "Cut-Cornered " in pdf_dc['shape'] or "square" in pdf_dc['shape']:
                type[0] = "radiant sq"
            elif "radiant" in pdf_dc['shape'].lower():
                type[0] = "radiant sq"
            elif "brilliant pear" in pdf_dc['shape'].lower():
                type[0] = "pear"
            elif "pear brilliant" in pdf_dc['shape'].lower():
                type[0] = "pear"


        else:
            if affiliate_process_data is not None and affiliate_process_data:
                if affiliate_process_data["shape"] is not None:
                    pdf_dc['shape'] = affiliate_process_data["shape"]
                    if "shape" in pdf_dc:
                        if "shape" in pdf_dc:
                            type = pdf_dc['shape'].split()
                            if "Square Emerald" in pdf_dc['shape']:
                                type[0] = "asscher"
                            if "CUT EMERALD" in pdf_dc['shape']:
                                type[0] = "emerald"
                            elif "Square Modified" in pdf_dc['shape']:
                                type[0] = "princess"
                            elif "Cut-Cornered" in pdf_dc['shape'] and "Rectangular" in pdf_dc['shape']:
                                type[0] = "radiant rec"
                            elif "Cut-Cornered " in pdf_dc['shape'] and "square" in pdf_dc['shape']:
                                type[0] = "radiant sq"
                            elif 'cushion' in pdf_dc['shape']:
                                pdf_dc['shape'] = "cushion"
                                type[0] = "cushion"
                            elif type[0].lower() == 'cc':
                                type[0] = "cushion"
                            elif "Cut-Cornered " in pdf_dc['shape'] or "Rectangular" in pdf_dc['shape']:
                                type[0] = "radiant rec"
                            elif "Cut-Cornered " in pdf_dc['shape'] or "square" in pdf_dc['shape']:
                                type[0] = "radiant sq"
                            elif "radiant" in pdf_dc['shape'].lower():
                                type[0] = "radiant sq"


                            elif "brilliant" in pdf_dc['shape'].lower():
                                type[0] = "pear"
                            elif "brilliant pear" in pdf_dc['shape'].lower():
                                type[0] = "pear"
                            elif "pear brilliant" in pdf_dc['shape'].lower():
                                type[0] = "pear"


                        else:
                            pdf_dc['shape'] = "ND"

            pdf_dc['shape'] = "ND"
        if 'round' in pdf_dc['shape'].lower():
            return fetch_data_round(req_dc, pdf_dc, affiliate_process_data, shape=None)

        if pdf_dc["measurement"] is not None:
            # measurements = pdf_dc['measurement'].replace("*", "x")
            measurements = pdf_dc['measurement'].replace(",", ".").replace("*", "x").lower().split('x')
            if len(measurements) == 1:
                measurements = pdf_dc['measurement'].split('X')
            try:
                lw_ratio = float(measurements[0]) / float(measurements[1])
            except:
                try:
                    measurements_sp = measurements[0].split()
                    lw_ratio = float(measurements_sp[0]) / float(measurements_sp[1])
                except:
                    # measurements_sp = measurements[0].split()
                    pdf_dc['measurement'] = 'ND'
                    lw_ratio = 'ND'

            # Converting the above number into decimal
            try:
                decimal_value = decimal.Decimal(lw_ratio)
                # rounding off
                lw_ratio = decimal_value.quantize(decimal.Decimal('0.00'))
            except:
                if affiliate_process_data is not None and affiliate_process_data:

                    if affiliate_process_data["measurement"] is not None and affiliate_process_data[
                        "measurement"] != 'False' and \
                            affiliate_process_data["measurement"] != 'false' and affiliate_process_data[
                        "measurement"] != '' and \
                            affiliate_process_data["measurement"] != 'null' and affiliate_process_data[
                        "measurement"] != 'Null' and \
                            affiliate_process_data["measurement"] != 'none' and affiliate_process_data[
                        "measurement"] != 'None':
                        pdf_dc['measurement'] = affiliate_process_data["measurement"]
                        # measurements = pdf_dc['measurement'].replace("*", "x")
                        measurements = affiliate_process_data['measurement'].replace(",", ".").replace("*",
                                                                                                       "x").lower().split(
                            'x')
                        if len(measurements) == 1:
                            measurements = affiliate_process_data['measurement'].split('X')
                        try:
                            lw_ratio = float(measurements[0]) / float(measurements[1])
                        except:
                            try:
                                measurements_sp = measurements[0].split()
                                lw_ratio = float(measurements_sp[0]) / float(measurements_sp[1])
                            except:
                                pdf_dc['measurement'] = 'ND'
                                lw_ratio = 'ND'
                        try:
                            decimal_value = decimal.Decimal(lw_ratio)
                            # rounding off
                            lw_ratio = decimal_value.quantize(decimal.Decimal('0.00'))
                        except:
                            pdf_dc['measurement'] = "ND"

                    else:
                        pdf_dc['measurement'] = "ND"
                else:
                    pdf_dc['measurement'] = "ND"
        else:
            if affiliate_process_data is not None and affiliate_process_data:
                if affiliate_process_data["measurement"] is not None and affiliate_process_data[
                    "measurement"] != 'False' and \
                        affiliate_process_data["measurement"] != 'false' and affiliate_process_data[
                    "measurement"] != '' and \
                        affiliate_process_data["measurement"] != 'null' and affiliate_process_data[
                    "measurement"] != 'Null' and \
                        affiliate_process_data["measurement"] != 'none' and affiliate_process_data[
                    "measurement"] != 'None':
                    pdf_dc['measurement'] = affiliate_process_data["measurement"]
                    # measurements = pdf_dc['measurement'].replace("*", "x")
                    measurements = affiliate_process_data['measurement'].replace(",", ".").replace("*",
                                                                                                   "x").lower().split(
                        'x')
                    if len(measurements) == 1:
                        measurements = affiliate_process_data['measurement'].split('X')
                    try:
                        lw_ratio = float(measurements[0]) / float(measurements[1])
                    except:
                        try:
                            measurements_sp = measurements[0].split()
                            lw_ratio = float(measurements_sp[0]) / float(measurements_sp[1])
                        except:
                            pdf_dc['measurement'] = 'ND'
                            lw_ratio = 'ND'
                    try:
                        decimal_value = decimal.Decimal(lw_ratio)
                        # rounding off
                        lw_ratio = decimal_value.quantize(decimal.Decimal('0.00'))
                    except:
                        pdf_dc['measurement'] = "ND"
                else:
                    pdf_dc['measurement'] = "ND"
            else:
                pdf_dc['measurement'] = "ND"

        final_dict = {}
        # shape = type[0].lower()
        if shape is None and "not" not in pdf_dc['shape']:
            req_dict = next(filter(lambda diamond_dict: diamond_dict["Diamond_type"] == type[0].lower(), req_dc), None)
            req_dict = req_dict['Diamonds_req'] if req_dict else None

            try:
                ratio = req_dict['length_width_ratio'].split('-')
            except:
                ratio = None
        # else:
        #     req_dict = next(filter(lambda diamond_dict: diamond_dict["Diamond_type"] == shape, req_dc), None)

        # if pdf_dc['table_size'] is not None:
        #     table = req_dict['table'].split('-')
        # if pdf_dc['depth'] is not None:
        #     depth = req_dict['depth'].split('-')

        if pdf_dc['girdle'] is not None:
            girdle_gia = pdf_dc['girdle']
        else:
            if affiliate_process_data is not None and affiliate_process_data:
                if affiliate_process_data["griddle"] is not None and affiliate_process_data["griddle"] != 'false' and \
                        affiliate_process_data["griddle"] != 'False' and affiliate_process_data["griddle"] != '' and \
                        affiliate_process_data["griddle"] != 'none' and affiliate_process_data["griddle"] != 'None' and \
                        affiliate_process_data["griddle"] != 'null' and affiliate_process_data["griddle"] != 'Null':
                    pdf_dc['girdle'] = affiliate_process_data['griddle']
                    girdle_gia = pdf_dc['girdle']
                else:
                    pdf_dc['girdle'] = "ND"
                    girdle_gia = 'None'
            else:
                pdf_dc['girdle'] = "ND"
                girdle_gia = 'None'

        if pdf_dc['polish'] is not None:
            polish_value = pdf_dc['polish'].lower()
            # thres_polish = req_dict['polish']

        else:
            if affiliate_process_data is not None and affiliate_process_data:
                if affiliate_process_data["polish"] is not None and affiliate_process_data["polish"] != 'false' and \
                        affiliate_process_data["polish"] != 'False' and affiliate_process_data["polish"] != '' and \
                        affiliate_process_data["polish"] != 'None' and affiliate_process_data["polish"] != 'none' and \
                        affiliate_process_data["polish"] != 'null' and affiliate_process_data["polish"] != 'Null':
                    pdf_dc['polish'] = affiliate_process_data['polish']
                    # polish_value = pdf_dc['polish'].lower()
                    polish_value = affiliate_process_data['polish'].lower()
                else:
                    polish_value = 'None'
                    pdf_dc['polish'] = "ND"

            else:
                polish_value = 'None'
                pdf_dc['polish'] = "ND"
        # thres_polish = req_dict['polish']
        if pdf_dc['symmetry'] is not None:
            symmetry_value = pdf_dc['symmetry']
        else:
            # thres_symmetry = req_dict['symmetry']
            if affiliate_process_data is not None and affiliate_process_data:
                if affiliate_process_data["symmetry"] is not None and affiliate_process_data["symmetry"] != '' and \
                        affiliate_process_data["symmetry"] != 'false' and affiliate_process_data[
                    "symmetry"] != 'False' and \
                        affiliate_process_data["symmetry"] != 'none' and affiliate_process_data[
                    "symmetry"] != 'None' and \
                        affiliate_process_data["symmetry"] != 'null' and affiliate_process_data["symmetry"] != 'Null':
                    pdf_dc['symmetry'] = affiliate_process_data['symmetry']
                    symmetry_value = affiliate_process_data['symmetry']
                else:
                    symmetry_value = 'None'
                    pdf_dc['symmetry'] = "ND"
            else:
                symmetry_value = 'None'
                pdf_dc['symmetry'] = "ND"

        # thres_symmetry = req_dict['symmetry']
        # cut_grade_value = pdf_dc["cut_grade"]
        if pdf_dc['carat'] is None:
            if affiliate_process_data is not None and affiliate_process_data:
                if affiliate_process_data['carat'] is not None:
                    pdf_dc['carat'] = affiliate_process_data['carat']

        if 'culet' in pdf_dc and pdf_dc['culet'] is not None:
            culet_point = check_culet(type[0], pdf_dc["culet"], characteristic_data)
            final_dict['culet_score'] = culet_point
        else:
            if affiliate_process_data is not None and affiliate_process_data:
                if affiliate_process_data["culet"] is not None and affiliate_process_data["culet"] != '' and \
                        affiliate_process_data["culet"] != 'false' and affiliate_process_data["culet"] != 'False' and \
                        affiliate_process_data["culet"] != 'null' and affiliate_process_data["culet"] != 'Null':
                    pdf_dc['culet'] = affiliate_process_data["culet"]
                    culet_point = check_culet(type[0], affiliate_process_data["culet"], characteristic_data)
                    final_dict['culet_score'] = culet_point
                else:
                    pdf_dc['culet'] = "ND"
                    final_dict['culet_score'] = 0
            else:
                pdf_dc['culet'] = "ND"
                final_dict['culet_score'] = 0
        # if "measurement" not in pdf_dc["measurement"] or pdf_dc["measurement"] == "ND":
        #     final_dict['length_width_ratio_score'] = 0
        try:
            if ratio is not None:

                if float(lw_ratio) >= float(ratio[0]) and lw_ratio <= float(ratio[1]):
                    final_dict['length_width_ratio_score'] = 5
                else:
                    final_dict['length_width_ratio_score'] = 0
            else:
                pass
                # final_dict['length_width_ratio_score'] = 'Not Applicable'

        except:
            final_dict['length_width_ratio_score'] = 0

        if pdf_dc['table_size'] is not None:
            table = req_dict['table'].split('-')
            try:
                pdf_dc['table_size'] = pdf_dc['table_size'].split("%")[0]
            except:
                pass

            if pdf_dc['table_size'] == '0' or pdf_dc['table_size'] == 'ND' or pdf_dc['table_size'] == 'Not Applicable':
                table_size_value = 0
            else:
                table_size_value = float(re.findall(pattern_digi, pdf_dc["table_size"])[0].replace(',', '.'))
            if table_size_value >= float(table[0]) and table_size_value <= float(table[1]):
                final_dict['table_size_score'] = 5
            else:
                final_dict['table_size_score'] = 0
        else:
            if affiliate_process_data is not None and affiliate_process_data:
                if affiliate_process_data["table_size"] is not None and affiliate_process_data[
                    "table_size"] != 'Null' and \
                        affiliate_process_data["table_size"] != 'none' and affiliate_process_data[
                    "table_size"] != '' and \
                        affiliate_process_data["table_size"] != 'None' and affiliate_process_data[
                    "table_size"] != 'false' and \
                        affiliate_process_data["table_size"] != 'False':
                    pdf_dc['table_size'] = affiliate_process_data['table_size']
                    pdf_dc['table_size'] = pdf_dc['table_size'].split("%")[0]
                    table = req_dict['table'].split('-')
                    table_size_value = float(
                        re.findall(pattern_digi, affiliate_process_data["table_size"])[0].replace(',', '.'))
                    if table_size_value >= float(table[0]) and table_size_value <= float(table[1]):
                        final_dict['table_size_score'] = 5
                    else:
                        final_dict['table_size_score'] = 0
                else:
                    pdf_dc['table_size'] = "ND"
                    final_dict['table_size_score'] = 0

            else:
                pdf_dc['table_size'] = "ND"
                final_dict['table_size_score'] = 0
        # else:
        #     pdf_dc['table_size'] = "ND"
        #     final_dict['table_size_score'] = 0
        if pdf_dc['depth'] is not None:
            depth = req_dict['depth'].split('-')
            if pdf_dc["depth"] == '0' or pdf_dc["depth"] == 'ND' or pdf_dc["depth"] == 'Not Applicable':
                depth_value = 0
            else:
                depth_value = float(re.findall(pattern_digi, pdf_dc["depth"])[0].replace(',', '.'))
            if depth_value >= float(depth[0]) and depth_value <= float(depth[1]):
                final_dict['depth_score'] = 5
            else:
                final_dict['depth_score'] = 0
        else:
            if affiliate_process_data is not None and affiliate_process_data:
                if affiliate_process_data["depth"] is not None and affiliate_process_data["depth"] != 'null' and \
                        affiliate_process_data["depth"] != 'Null' and affiliate_process_data[
                    "depth"] != 'false' and affiliate_process_data["depth"] != 'False' and affiliate_process_data[
                    "depth"] != 'none' and affiliate_process_data["depth"] != 'None' and affiliate_process_data[
                    "depth"] != '':
                    pdf_dc["depth"] = affiliate_process_data['depth']
                    depth = req_dict['depth'].split('-')
                    depth_value = float(re.findall(pattern_digi, pdf_dc["depth"])[0].replace(',', '.'))
                    if depth_value >= float(depth[0]) and depth_value <= float(depth[1]):
                        final_dict['depth_score'] = 5
                    else:
                        final_dict['depth_score'] = 0
                else:
                    pdf_dc['depth'] = "ND"
                    final_dict['depth_score'] = 0
            else:
                pdf_dc['depth'] = "ND"
                final_dict['depth_score'] = 0

        # else:
        #     pdf_dc['depth'] = "ND"
        # assign_girdle_value_for_heart(girdle_gia, characteristic_data)
        #     final_dict['depth_score'] = 0
        if affiliate_process_data is not None and affiliate_process_data:
            if affiliate_process_data['shape'] is not None:
                pdf_dc['shape'] = affiliate_process_data['shape']
        # else:
        #     if pdf_dc["shape"] != "ND" or pdf_dc["shape"] is not None:

        if pdf_dc["measurement"] == "ND" or pdf_dc["measurement"] is None:
            pass
        else:
            if ratio is not None:
                pdf_dc['length_width_ratio'] = float(lw_ratio)

        if pdf_dc['girdle'] == 'none' or pdf_dc['girdle'] == 'false' or pdf_dc['girdle'] == 'None':
            pdf_dc['girdle'] = "ND"
        if pdf_dc['girdle'] == "ND" or pdf_dc["girdle"] is None:
            final_dict['girdle_score'] = 0
        else:
            if pdf_dc["shape"] != "ND" or pdf_dc["shape"] is not None:
                if 'heart' in pdf_dc['shape'].lower():
                    girdle_point = assign_girdle_value_for_heart(girdle_gia, characteristic_data)
                else:
                    girdle_point = assign_girdle_value(girdle_gia, characteristic_data)
            else:
                girdle_point = assign_girdle_value(girdle_gia, characteristic_data)
            # girdle_point = check_girdle(gridle, girdle_gia, characteristic_data)
            final_dict['girdle_score'] = girdle_point

        if pdf_dc['polish'] == "ND" or pdf_dc['polish'] is None:
            final_dict['polish_score'] = 0
        else:
            # polish_value = "fair"
            polish_point = check_polish("None", polish_value, characteristic_data)
            final_dict['polish_score'] = polish_point
        if pdf_dc['symmetry'] == 'ND' or pdf_dc["symmetry"] is None:
            final_dict['symmetry_score'] = 0
        else:
            symmetry_point = check_symmetry('None', symmetry_value, characteristic_data)
            final_dict['symmetry_score'] = symmetry_point

        if pdf_dc['fluorescence'] is not None and pdf_dc['color_grade'] is not None:
            value = check_fluorescence(pdf_dc['fluorescence'], characteristic_data, pdf_dc['color_grade'])
            if value is None:
                # pass
                final_dict['fluorescence_score'] = 0
            else:
                final_dict['fluorescence_score'] = value

        else:
            if affiliate_process_data is not None:
                if affiliate_process_data['fluorescence'] is not None and affiliate_process_data[
                    'color_grade'] is not None:
                    pdf_dc['fluorescence'] = affiliate_process_data['fluorescence']
                    pdf_dc['color_grade'] = affiliate_process_data['color_grade']

                    value = check_fluorescence(pdf_dc['fluorescence'], characteristic_data, pdf_dc['color_grade'])
                    if value is None:
                        # pass
                        final_dict['fluorescence_score'] = 0
                    else:
                        final_dict['fluorescence_score'] = value
                else:
                    final_dict['fluorescence_score'] = 0
                    if pdf_dc['fluorescence'] is None:
                        pdf_dc["fluorescence"] = 'ND'
                    if pdf_dc['color_grade'] is None:
                        pdf_dc["color_grade"] = 'ND'
            else:
                final_dict['fluorescence_score'] = 0
                if pdf_dc['fluorescence'] is None:
                    pdf_dc["fluorescence"] = 'ND'
                if pdf_dc['color_grade'] is None:
                    pdf_dc["color_grade"] = 'ND'

        if pdf_dc["key_to_symbol"] is not None:
            symbol_value = check_symbol(pdf_dc['key_to_symbol'], characteristic_data)
            values = symbol_value
            final_dict['key_to_symbol_score'] = values
        else:
            final_dict['key_to_symbol_score'] = 5

        total_marks = sum(list(final_dict.values()))
        overall_marks = ((int(len(final_dict.values()))) * 5)
        percent = (total_marks / overall_marks) * 100
        # final_dict['symbol_score'] = None
        final_dict['digisation_score'] = f'{round(percent, 2)}%'
        logger.debug(
            "Non-round fetch_data completed for shape=%s with digisation_score=%s",
            pdf_dc.get("shape"),
            final_dict['digisation_score'],
        )
        score_dict = score_info_dict(final_dict)
        score_dict, pdf_dc = dict_np(score_dict, pdf_dc)
        return score_dict, pdf_dc_new, pdf_dc
    else:
        logger.warning("fetch_data: cannot determine shape from data, returning error status")
        return {"Status": "can't find shape from data"}, pdf_dc_new, pdf_dc


def assign_girdle_value(value, characteristic_json):
    info = None
    assign_value = 0
    for data in characteristic_json:
        if data['characteristic_name'] == "GIRDLE THICKNESS":
            info = data['characteristic_data']
            break
    for i in info:
        if i['name'].lower() == value.lower():
            assign_value = i['value']
            return assign_value
    return assign_value


def assign_girdle_value_for_heart(value, characteristic_json):
    info = None
    assign_value = 0
    for data in characteristic_json:
        if data['characteristic_name'] == "GIRDLE THICKNESS HEART":
            info = data['characteristic_data']
            break
    for i in info:
        if i['name'].lower() == value.lower():
            assign_value = i['value']
            return assign_value
    return assign_value


def assign_measurement_value(value, carat, characteristic_json):
    info = None
    assigned_value = 0
    for data in characteristic_json:
        if data['characteristic_name'] == "measurement":
            info = data['characteristic_data']
            break
    for i in info:
        w = i['weight'].split('-')
        weight_range = i["weight"].split("-")
        min_weight = float(weight_range[0])
        max_weight = float(weight_range[1])

        # Check if the carat value lies within the range
        if min_weight <= carat <= max_weight:
            orginal_value = i["value"]
            if value >= orginal_value:
                assigned_value = 5
                return assigned_value
        if carat >= 2.0:
            if value >= 8:
                assigned_value = 5

    return assigned_value


def check_cut_grade(threshold="None", value=None, characteristic_json=None):
    info = None
    for data in characteristic_json:
        if data['characteristic_name'] == "ROUND SCORE":
            info = data['characteristic_data']
            break
    if value.lower() in threshold and threshold != 'None':
        for i in info:
            if i['data_type'].lower() == value.lower():
                config = i['type_configuration'][1]
                point = config['value']
                return point
    else:
        for i in info:
            if i['data_type'].lower() == value.lower():
                config = i['type_configuration'][1]
                point = config['value']
                return point
    return 0


def get_pavillion_angle(pdf_data, characteristic_data):
    info = None
    for data in characteristic_data:
        if data['characteristic_name'] == "pavillion_angle":
            info = data['characteristic_data']
            break
    info_keys = list(info.keys())
    for key in info_keys:
        pav_data = key.split('-')
        if len(pav_data) == 2:
            if pdf_data >= float(pav_data[0]) and pdf_data <= float(pav_data[1]):
                return info[key]
        else:
            if pdf_data == float(pav_data[0]):
                return info[key]
    return 0


def get_round_crown(pdf_data, characteristic_data):
    info = None
    for data in characteristic_data:
        if data['characteristic_name'] == "round_crown":
            info = data['characteristic_data']
            break
    info_keys = list(info.keys())
    for key in info_keys:
        pav_data = key.split('-')
        if len(pav_data) == 2:
            if pdf_data >= float(pav_data[0]) and pdf_data <= float(pav_data[1]):
                return info[key]
        else:
            if pdf_data == float(pav_data[0]):
                return info[key]

    return 0


def get_configuration_json(pdf_dc, shape=None):
    type = pdf_dc['shape'].split()
    # type = pdf_dc['shape_and_style'].split()
    if "Square Emerald" in pdf_dc['shape']:
        type[0] = "asscher"
    if "Square Modified" in pdf_dc['shape']:
        type[0] = "princess"
    if "Cut-Cornered" in pdf_dc['shape'] and "Rectangular" in pdf_dc['shape']:
        type[0] = "radiant rec"
    if "Cut-Cornered " in pdf_dc['shape'] and "square" in pdf_dc['shape']:
        type[0] = "radiant sq"
    req_dict = next(filter(lambda diamond_dict: diamond_dict["Diamond_type"] == type[0].lower(), data), None)
    return req_dict


def check_culet(shape, culet_value, characteristic_json):
    info = None
    for data in characteristic_json:
        if data['characteristic_name'] == "culet":
            info = data['characteristic_data']
            break
    for d in info:
        if d["shape"].lower() in shape.lower().strip():
            for value in d["value"]:
                if value["name"].upper() == culet_value.upper():
                    return value["value"]
    return 0


def fetch_data_round(req_dc, pdf_dc, affiliate_process_data, shape=None):
    logger.debug(
        "Starting round fetch_data_round for shape=%s with affiliate_process_data=%s",
        pdf_dc.get("shape"),
        bool(affiliate_process_data),
    )
    pdf_dc_new = pdf_dc.copy()
    if affiliate_process_data is not None and affiliate_process_data:
        if affiliate_process_data["shape"] is not None and affiliate_process_data["shape"] != '' and \
                affiliate_process_data["shape"] != 'null' and affiliate_process_data["shape"] != 'Null' and \
                affiliate_process_data["shape"] != 'False' and affiliate_process_data["shape"] != 'false':
            pdf_dc['shape'] = affiliate_process_data["shape"]
        if affiliate_process_data["carat"] is not None and affiliate_process_data["carat"] != 'false' and \
                affiliate_process_data["carat"] != '' and affiliate_process_data["carat"] != 'none' and \
                affiliate_process_data["carat"] != 'None' and affiliate_process_data["carat"] != 'null' and \
                affiliate_process_data["carat"] != 'Null':
            pdf_dc['carat'] = affiliate_process_data["carat"]
        if affiliate_process_data["table_size"] is not None and affiliate_process_data["table_size"] != 'Null' and \
                affiliate_process_data["table_size"] != 'none' and affiliate_process_data["table_size"] != '' and \
                affiliate_process_data["table_size"] != 'None' and affiliate_process_data["table_size"] != 'false' and \
                affiliate_process_data["table_size"] != 'False':
            pdf_dc['table_size'] = affiliate_process_data["table_size"]
        if affiliate_process_data["griddle"] is not None and affiliate_process_data["griddle"] != 'false' and \
                affiliate_process_data["griddle"] != 'False' and affiliate_process_data["griddle"] != '' and \
                affiliate_process_data["griddle"] != 'none' and affiliate_process_data["griddle"] != 'None' and \
                affiliate_process_data["griddle"] != 'null' and affiliate_process_data["griddle"] != 'Null':
            pdf_dc['girdle'] = affiliate_process_data["griddle"]
        if affiliate_process_data["depth"] is not None and affiliate_process_data["depth"] != 'null' and \
                affiliate_process_data["depth"] != 'Null' and affiliate_process_data[
            "depth"] != 'false' and affiliate_process_data["depth"] != 'False' and affiliate_process_data[
            "depth"] != 'none' and affiliate_process_data["depth"] != 'None' and affiliate_process_data["depth"] != '':
            pdf_dc['depth'] = affiliate_process_data["depth"]
        if affiliate_process_data["measurement"] is not None and affiliate_process_data["measurement"] != 'False' and \
                affiliate_process_data["measurement"] != 'false' and affiliate_process_data["measurement"] != '' and \
                affiliate_process_data["measurement"] != 'null' and affiliate_process_data["measurement"] != 'Null' and \
                affiliate_process_data["measurement"] != 'none' and affiliate_process_data["measurement"] != 'None':
            pdf_dc['measurement'] = affiliate_process_data["measurement"]
        if affiliate_process_data["culet"] is not None and affiliate_process_data["culet"] != '' and \
                affiliate_process_data["culet"] != 'false' and affiliate_process_data["culet"] != 'False' and \
                affiliate_process_data["culet"] != 'null' and affiliate_process_data["culet"] != 'Null':
            pdf_dc['culet'] = affiliate_process_data["culet"]

        if affiliate_process_data["color"] is not None and affiliate_process_data["color"] != 'none' and \
                affiliate_process_data["color"] != 'None' and affiliate_process_data["color"] != '' and \
                affiliate_process_data["color"] != 'False' and affiliate_process_data["color"] != 'false' and \
                affiliate_process_data["color"] != 'null' and affiliate_process_data["color"] != 'Null':
            pdf_dc['color_grade'] = affiliate_process_data["color"]

        if affiliate_process_data["polish"] is not None and affiliate_process_data["polish"] != 'false' and \
                affiliate_process_data["polish"] != 'False' and affiliate_process_data["polish"] != '' and \
                affiliate_process_data["polish"] != 'None' and affiliate_process_data["polish"] != 'none' and \
                affiliate_process_data["polish"] != 'null' and affiliate_process_data["polish"] != 'Null':
            pdf_dc['polish'] = affiliate_process_data["polish"]

        if affiliate_process_data["symmetry"] is not None and affiliate_process_data["symmetry"] != '' and \
                affiliate_process_data["symmetry"] != 'false' and affiliate_process_data["symmetry"] != 'False' and \
                affiliate_process_data["symmetry"] != 'none' and affiliate_process_data["symmetry"] != 'None' and \
                affiliate_process_data["symmetry"] != 'null' and affiliate_process_data["symmetry"] != 'Null':
            pdf_dc['symmetry'] = affiliate_process_data["symmetry"]
        if affiliate_process_data["fluorescence"] is not None and affiliate_process_data["fluorescence"] != '' and \
                affiliate_process_data["fluorescence"] != 'False' and affiliate_process_data[
            "fluorescence"] != 'false' and affiliate_process_data["fluorescence"] != 'null' and affiliate_process_data[
            "fluorescence"] != 'Null':
            pdf_dc['fluorescence'] = affiliate_process_data["fluorescence"]
        if affiliate_process_data["cut"] is not None and affiliate_process_data["cut"] != '' and affiliate_process_data[
            "cut"] != 'none' and affiliate_process_data["cut"] != 'None' and affiliate_process_data[
            "cut"] != 'false' and affiliate_process_data["cut"] != 'False' and affiliate_process_data[
            "cut"] != 'null' and affiliate_process_data["cut"] != 'Null':
            pdf_dc['cut'] = affiliate_process_data["cut"]
        if affiliate_process_data["pavilion_height"] is not None and affiliate_process_data[
            "pavilion_height"] != 'false' and affiliate_process_data["pavilion_height"] != '' and \
                affiliate_process_data["pavilion_height"] != 'False' and affiliate_process_data[
            "pavilion_height"] != 'none' and affiliate_process_data["pavilion_height"] != 'None' and \
                affiliate_process_data["pavilion_height"] != 'null' and affiliate_process_data[
            "pavilion_height"] != 'Null' and affiliate_process_data["pavilion_height"] != '0.0000':
            pdf_dc['pavilion_height'] = affiliate_process_data["pavilion_height"]
        if affiliate_process_data["pavilion_angle"] is not None and affiliate_process_data[
            "pavilion_angle"] != 'false' and affiliate_process_data["pavilion_angle"] != '' and affiliate_process_data[
            "pavilion_angle"] != 'False' and affiliate_process_data["pavilion_angle"] != 'none' and \
                affiliate_process_data["pavilion_angle"] != 'None' and affiliate_process_data[
            "pavilion_angle"] != 'null' and affiliate_process_data["pavilion_angle"] != 'Null' and \
                affiliate_process_data["pavilion_angle"] != '0.0000':
            pdf_dc['pavilion_angle'] = affiliate_process_data["pavilion_angle"]
        if affiliate_process_data["crown_height"] is not None and affiliate_process_data["crown_height"] != '' and \
                affiliate_process_data["crown_height"] != 'false' and affiliate_process_data[
            "crown_height"] != 'False' and affiliate_process_data["crown_height"] != 'none' and affiliate_process_data[
            "crown_height"] != 'None' and affiliate_process_data["crown_height"] != 'null' and affiliate_process_data[
            "crown_height"] != 'Null':
            pdf_dc['crown_height'] = affiliate_process_data["crown_height"]
        if affiliate_process_data["crown_angle"] is not None and affiliate_process_data["crown_angle"] != '' and \
                affiliate_process_data["crown_angle"] != 'False' and affiliate_process_data[
            "crown_angle"] != 'false' and affiliate_process_data["crown_angle"] != 'none' and affiliate_process_data[
            "crown_angle"] != 'None' and affiliate_process_data["crown_angle"] != 'null' and affiliate_process_data[
            "crown_angle"] != 'Null':
            pdf_dc['crown_angle'] = affiliate_process_data["crown_angle"]
    if 'shape' in pdf_dc:
        type = pdf_dc['shape'].split()
        last_mea = None
        if 'measurement' in pdf_dc:
            if pdf_dc['measurement'] is not None:
                last_mea = pdf_dc['measurement'].split('*')[0]
                last_mea = last_mea.split('x')[0]
                last_mea = last_mea.split('X')[0]
        else:
            if affiliate_process_data is not None and affiliate_process_data:
                if affiliate_process_data["measurement"] is not None and affiliate_process_data[
                    "measurement"] != 'False' and \
                        affiliate_process_data["measurement"] != 'false' and affiliate_process_data[
                    "measurement"] != '' and \
                        affiliate_process_data["measurement"] != 'null' and affiliate_process_data[
                    "measurement"] != 'Null' and \
                        affiliate_process_data["measurement"] != 'none' and affiliate_process_data[
                    "measurement"] != 'None':

                    pdf_dc['measurement'] = affiliate_process_data['measurement']
                    if 'measurement' in pdf_dc:
                        if pdf_dc['measurement'] is not None:
                            last_mea = pdf_dc['measurement'].split('*')[0]
                            last_mea = last_mea.split('x')[0]
                            last_mea = last_mea.split('X')[0]

        final_dict = {}

        if shape is None:
            req_dict = next(filter(lambda diamond_dict: diamond_dict["Diamond_type"] == type[0].lower(), req_dc), None)
        else:
            req_dict = next(filter(lambda diamond_dict: diamond_dict["Diamond_type"] == shape, req_dc), None)

        req_dict = req_dict['Diamonds_req'] if req_dict else None
        table = req_dict['table'].split('-')
        depth = req_dict['depth'].split('-')
        pavilion_angle = req_dict['pavilion_angle'].split('-')
        pavilion_depth = req_dict['pavilion_depth'].split('-')
        # crown_angle = req_dict['crown_angle'].split('-')
        if 'girdle' in pdf_dc and pdf_dc['girdle'] is not None:
            gridle_ls = pdf_dc['girdle'].split()
            if len(gridle_ls) != 1:
                if "(" in gridle_ls[-2]:
                    del gridle_ls[-2]
                if "%" in gridle_ls[-1]:
                    del gridle_ls[-1]
                girdle_gia = ' '.join(gridle_ls)
            else:
                girdle_gia = pdf_dc['girdle']
        else:
            if affiliate_process_data is not None and affiliate_process_data:
                if affiliate_process_data["griddle"] is not None and affiliate_process_data["griddle"] != 'false' and \
                        affiliate_process_data["griddle"] != 'False' and affiliate_process_data["griddle"] != '' and \
                        affiliate_process_data["griddle"] != 'none' and affiliate_process_data["griddle"] != 'None' and \
                        affiliate_process_data["griddle"] != 'null' and affiliate_process_data["griddle"] != 'Null':

                    pdf_dc['girdle'] = affiliate_process_data['girdle']
                    if 'girdle' in pdf_dc and pdf_dc['girdle'] is not None:
                        gridle_ls = pdf_dc['girdle'].split()
                        if len(gridle_ls) != 1:
                            if "(" in gridle_ls[-2]:
                                del gridle_ls[-2]
                            if "%" in gridle_ls[-1]:
                                del gridle_ls[-1]
                            girdle_gia = ' '.join(gridle_ls)
                        else:
                            girdle_gia = pdf_dc['girdle']
                    else:
                        pdf_dc['girdle'] = "ND"
                        girdle_gia = 'None'
                else:
                    pdf_dc['girdle'] = "ND"
                    girdle_gia = 'None'
            else:
                pdf_dc['girdle'] = "ND"
                girdle_gia = 'None'
        if 'polish' in pdf_dc and pdf_dc["polish"] is not None:
            polish_value = pdf_dc['polish']

        else:
            if affiliate_process_data is not None and affiliate_process_data:
                if affiliate_process_data["polish"] is not None and affiliate_process_data["polish"] != 'false' and \
                        affiliate_process_data["polish"] != 'False' and affiliate_process_data["polish"] != '' and \
                        affiliate_process_data["polish"] != 'None' and affiliate_process_data["polish"] != 'none' and \
                        affiliate_process_data["polish"] != 'null' and affiliate_process_data["polish"] != 'Null':
                    pdf_dc['polish'] = affiliate_process_data['polish']
                    if 'polish' in pdf_dc and pdf_dc["polish"] is not None:
                        polish_value = pdf_dc['polish']
                    else:
                        polish_value = 'None'
                        pdf_dc['polish'] = "ND"
                else:
                    polish_value = 'None'
                    pdf_dc['polish'] = "ND"

            else:
                polish_value = 'None'
                pdf_dc['polish'] = "ND"
        # thres_polish = req_dict['polish']
        if 'symmetry' in pdf_dc and pdf_dc["symmetry"] is not None:
            symmetry_value = pdf_dc['symmetry']
        else:
            if affiliate_process_data is not None and affiliate_process_data:
                if affiliate_process_data["symmetry"] is not None and affiliate_process_data["symmetry"] != '' and \
                        affiliate_process_data["symmetry"] != 'false' and affiliate_process_data[
                    "symmetry"] != 'False' and \
                        affiliate_process_data["symmetry"] != 'none' and affiliate_process_data[
                    "symmetry"] != 'None' and \
                        affiliate_process_data["symmetry"] != 'null' and affiliate_process_data["symmetry"] != 'Null':
                    pdf_dc['symmetry'] = affiliate_process_data['symmetry']
                    if 'symmetry' in pdf_dc and pdf_dc["symmetry"] is not None:
                        symmetry_value = pdf_dc['symmetry']
                    else:
                        symmetry_value = 'None'
                        pdf_dc['symmetry'] = "ND"
                else:
                    symmetry_value = 'None'
                    pdf_dc['symmetry'] = "ND"
            else:
                symmetry_value = 'None'
                pdf_dc['symmetry'] = "ND"

        if 'cut' in pdf_dc and pdf_dc["cut"] is not None:
            cut_grade_value = pdf_dc['cut']
        else:
            if affiliate_process_data is not None and affiliate_process_data:
                if affiliate_process_data["cut"] is not None and affiliate_process_data["cut"] != '' and \
                        affiliate_process_data[
                            "cut"] != 'none' and affiliate_process_data["cut"] != 'None' and affiliate_process_data[
                    "cut"] != 'false' and affiliate_process_data["cut"] != 'False' and affiliate_process_data[
                    "cut"] != 'null' and affiliate_process_data["cut"] != 'Null':
                    pdf_dc['cut'] = affiliate_process_data['cut']
                    if 'cut' in pdf_dc and pdf_dc["cut"] is not None:
                        cut_grade_value = pdf_dc['cut']
                    else:
                        cut_grade_value = 'None'
                        pdf_dc['cut'] = "ND"
                else:
                    cut_grade_value = 'None'
                    pdf_dc['cut'] = "ND"
            else:
                cut_grade_value = 'None'
                pdf_dc['cut'] = "ND"

        if "culet" in pdf_dc and pdf_dc['culet'] is not None:
            culet_point = check_culet("round", pdf_dc["culet"], characteristic_data)
            final_dict['culet_score'] = culet_point

        else:
            if affiliate_process_data is not None and affiliate_process_data:
                if affiliate_process_data["culet"] is not None and affiliate_process_data["culet"] != '' and \
                        affiliate_process_data["culet"] != 'false' and affiliate_process_data["culet"] != 'False' and \
                        affiliate_process_data["culet"] != 'null' and affiliate_process_data["culet"] != 'Null':
                    pdf_dc['culet'] = affiliate_process_data['culet']
                    if "culet" in pdf_dc and pdf_dc['culet'] is not None:
                        culet_point = check_culet("round", pdf_dc["culet"], characteristic_data)
                        final_dict['culet_score'] = culet_point
                    else:
                        pdf_dc["culet"] = "ND"
                        final_dict['culet_score'] = 0
                else:
                    pdf_dc["culet"] = "ND"
                    final_dict['culet_score'] = 0
            else:
                pdf_dc["culet"] = "ND"
                final_dict['culet_score'] = 0


        carat = 0
        if pdf_dc["carat"] is not None:
            try:
                carat = float(pdf_dc["carat"].split(' ')[0])
            except:
                carat = pdf_dc["carat"]

        if affiliate_process_data is not None and affiliate_process_data:
            if affiliate_process_data["carat"] is not None and affiliate_process_data["carat"] != 'false' and \
                    affiliate_process_data["carat"] != '' and affiliate_process_data["carat"] != 'none' and \
                    affiliate_process_data["carat"] != 'None' and affiliate_process_data["carat"] != 'null' and \
                    affiliate_process_data["carat"] != 'Null':
                carat = affiliate_process_data['carat']
                pdf_dc["carat"] = affiliate_process_data['carat']
            else:
                pdf_dc["carat"] = 0

        if 'measurement' in pdf_dc and pdf_dc['measurement'] is not None:
            if last_mea is not None:
                try:
                    measurement_score = assign_measurement_value(float(last_mea), carat, characteristic_data)
                    final_dict['measurement_score'] = measurement_score
                except:
                    final_dict['measurement_score'] = 0
            else:
                final_dict['measurement_score'] = 0

        else:
            pdf_dc["measurement"] = "ND"
            final_dict['measurement_score'] = 0

        if 'pavilion_height' in pdf_dc and pdf_dc["pavilion_height"] is not None and pdf_dc[
            "pavilion_height"] != 'false' and pdf_dc["pavilion_height"] != '' and pdf_dc["pavilion_height"] != 'ND' and \
                pdf_dc['pavilion_height'] != 'Not Applicable' and pdf_dc['pavilion_height'] != '1':
            pavillion_value = float(re.findall(pattern_digi, pdf_dc["pavilion_height"])[0].replace(',', '.'))
            if pavillion_value >= float(pavilion_depth[0]) and pavillion_value <= float(pavilion_depth[1]):
                final_dict['pavilion_height_score'] = 5
            else:
                final_dict['pavilion_height_score'] = 0
        else:
            if affiliate_process_data is not None and affiliate_process_data:
                pdf_dc['pavilion_height'] = affiliate_process_data['pavilion_height']
                if 'pavilion_height' in pdf_dc and pdf_dc["pavilion_height"] is not None and pdf_dc[
                    "pavilion_height"] != 'false' and pdf_dc["pavilion_height"] != '' and pdf_dc[
                    'pavilion_height'] != '1':
                    pavillion_value = float(re.findall(pattern_digi, pdf_dc["pavilion_height"])[0].replace(',', '.'))
                    if pavillion_value >= float(pavilion_depth[0]) and pavillion_value <= float(pavilion_depth[1]):
                        final_dict['pavilion_height_score'] = 5
                    else:
                        final_dict['pavilion_height_score'] = 0
                else:
                    pdf_dc["pavilion_height"] = "ND"
                    final_dict['pavilion_height_score'] = 0
            else:
                pdf_dc["pavilion_height"] = "ND"
                final_dict['pavilion_height_score'] = 0

        if "pavilion_angle" in pdf_dc and pdf_dc["pavilion_angle"] is not None and pdf_dc[
            "pavilion_angle"] != 'false' and pdf_dc["pavilion_angle"] != '' and pdf_dc["pavilion_angle"] != 'ND' and \
                pdf_dc["pavilion_angle"] != 'Not Applicable' and pdf_dc["pavilion_angle"] != '1':
            pavillion_angle_value = float(re.findall(pattern_digi, pdf_dc["pavilion_angle"])[0].replace(',', '.'))
            if pavillion_angle_value >= float(pavilion_angle[0]) and pavillion_angle_value <= float(pavilion_angle[1]):
                final_dict['pavilion_angle_score'] = 5
            else:
                pavilion_score = get_pavillion_angle(pavillion_angle_value, characteristic_data)
                final_dict['pavilion_angle_score'] = pavilion_score
        else:
            if affiliate_process_data is not None and affiliate_process_data:
                pdf_dc['pavilion_angle'] = affiliate_process_data['pavilion_angle']
                if "pavilion_angle" in pdf_dc and pdf_dc["pavilion_angle"] is not None and pdf_dc[
                    "pavilion_angle"] != 'false' and pdf_dc["pavilion_angle"] != '' and pdf_dc["pavilion_angle"] != '1':
                    pavillion_angle_value = float(
                        re.findall(pattern_digi, pdf_dc["pavilion_angle"])[0].replace(',', '.'))
                    if pavillion_angle_value >= float(pavilion_angle[0]) and pavillion_angle_value <= float(
                            pavilion_angle[1]):
                        final_dict['pavilion_angle_score'] = 5
                    else:
                        pavilion_score = get_pavillion_angle(pavillion_angle_value, characteristic_data)
                        final_dict['pavilion_angle_score'] = pavilion_score
                else:
                    # pdf_dc["pavilion_angle_score"] = "ND"
                    pdf_dc["pavilion_angle"] = "ND"
                    final_dict['pavilion_angle_score'] = 0
            else:
                pdf_dc["pavilion_angle"] = "ND"
                final_dict['pavilion_angle_score'] = 0

        if "crown_angle" in pdf_dc and pdf_dc["crown_angle"] is not None and pdf_dc["crown_angle"] != '' and pdf_dc[
            "crown_angle"] != 'false' and pdf_dc["crown_angle"] != 'none' and pdf_dc["crown_angle"] != 'ND' and pdf_dc[
            "crown_angle"] != 'Not Applicable' and pdf_dc["crown_angle"] != '1':
            crown_angle_value = float(re.findall(pattern_digi, pdf_dc["crown_angle"])[0].replace(',', '.'))

            crown_angle = req_dict['crown_angle'].split('-')

            if crown_angle_value >= float(crown_angle[0]) and crown_angle_value <= float(crown_angle[1]):
                final_dict['crown_angle_score'] = 5
            else:
                crown_angle_score = get_round_crown(crown_angle_value, characteristic_data)
                final_dict['crown_angle_score'] = crown_angle_score
        else:
            if affiliate_process_data is not None and affiliate_process_data:
                pdf_dc['crown_angle'] = affiliate_process_data['crown_angle']
                if "crown_angle" in pdf_dc and pdf_dc["crown_angle"] is not None and pdf_dc["crown_angle"] != '' and \
                        pdf_dc["crown_angle"] != 'false' and pdf_dc["crown_angle"] != 'none' and pdf_dc[
                    "crown_angle"] != 'ND' and pdf_dc["crown_angle"] != '1':
                    crown_angle = req_dict['crown_angle'].split('-')
                    crown_angle_value = float(re.findall(pattern_digi, pdf_dc["crown_angle"])[0].replace(',', '.'))
                    if crown_angle_value >= float(crown_angle[0]) and crown_angle_value <= float(crown_angle[1]):
                        final_dict['crown_angle_score'] = 5
                    else:
                        crown_angle_score = get_round_crown(crown_angle_value, characteristic_data)
                        final_dict['crown_angle_score'] = crown_angle_score

                else:
                    pdf_dc["crown_angle"] = "ND"
                    final_dict['crown_angle_score'] = 0
            else:
                pdf_dc["crown_angle"] = "ND"
                final_dict['crown_angle_score'] = 0

        if "table_size" in pdf_dc and pdf_dc["table_size"] is not None:
            if pdf_dc['table_size'] == '0' or pdf_dc["table_size"] == 'ND' or pdf_dc["table_size"] == 'Not Applicable':
                table_size_value = 0
            else:
                table_size_value = float(re.findall(pattern_digi, pdf_dc["table_size"])[0].replace(',', '.'))

            # table_size_value = float(re.findall(pattern_digi, pdf_dc["table_size"])[0].replace(',', '.'))
            if table_size_value >= float(table[0]) and table_size_value <= float(table[1]):
                final_dict['table_size_score'] = 5
            else:
                final_dict['table_size_score'] = 0
        else:
            if affiliate_process_data is not None and affiliate_process_data:
                if affiliate_process_data["table_size"] is not None and affiliate_process_data[
                    "table_size"] != 'Null' and \
                        affiliate_process_data["table_size"] != 'none' and affiliate_process_data[
                    "table_size"] != '' and \
                        affiliate_process_data["table_size"] != 'None' and affiliate_process_data[
                    "table_size"] != 'false' and \
                        affiliate_process_data["table_size"] != 'False':
                    pdf_dc['table_size'] = affiliate_process_data['table_size']
                    if "table_size" in pdf_dc and pdf_dc["table_size"] is not None:
                        if pdf_dc['table_size'] == '0':
                            table_size_value = 0
                        else:
                            table_size_value = float(
                                re.findall(pattern_digi, pdf_dc["table_size"])[0].replace(',', '.'))

                        # table_size_value = float(re.findall(pattern_digi, pdf_dc["table_size"])[0].replace(',', '.'))
                        if table_size_value >= float(table[0]) and table_size_value <= float(table[1]):
                            final_dict['table_size_score'] = 5
                        else:
                            final_dict['table_size_score'] = 0
                    else:
                        pdf_dc["table_size"] = "ND"
                        final_dict['table_size_score'] = 0
                else:
                    pdf_dc["table_size"] = "ND"
                    final_dict['table_size_score'] = 0
            else:
                pdf_dc["table_size"] = "ND"
                final_dict['table_size_score'] = 0

        if "depth" in pdf_dc and pdf_dc["depth"] is not None:
            if pdf_dc['depth'] == '0' or pdf_dc["depth"] == 'ND' or pdf_dc["depth"] == 'Not Applicable':
                depth_value = 0
            else:
                depth_value = float(re.findall(pattern_digi, pdf_dc["depth"])[0].replace(',', '.'))

            # depth_value = float(re.findall(pattern_digi, pdf_dc["depth"])[0].replace(',', '.'))
            if float(depth_value) >= float(depth[0]) and depth_value <= float(depth[1]):
                final_dict['depth_score'] = 5
            else:
                final_dict['depth_score'] = 0
        else:
            if affiliate_process_data is not None and affiliate_process_data:
                if affiliate_process_data["depth"] is not None and affiliate_process_data["depth"] != 'null' and \
                        affiliate_process_data["depth"] != 'Null' and affiliate_process_data[
                    "depth"] != 'false' and affiliate_process_data["depth"] != 'False' and affiliate_process_data[
                    "depth"] != 'none' and affiliate_process_data["depth"] != 'None' and affiliate_process_data[
                    "depth"] != '':
                    pdf_dc['depth'] = affiliate_process_data['depth']
                    if "depth" in pdf_dc and pdf_dc["depth"] is not None:
                        depth_value = float(re.findall(pattern_digi, pdf_dc["depth"])[0].replace(',', '.'))
                        if float(depth_value) >= float(depth[0]) and depth_value <= float(depth[1]):
                            final_dict['depth_score'] = 5
                        else:
                            final_dict['depth_score'] = 0
                    else:
                        pdf_dc["depth"] = "ND"
                        final_dict['depth_score'] = 0
                else:
                    pdf_dc["depth"] = "ND"
                    final_dict['depth_score'] = 0
            else:
                pdf_dc["depth"] = "ND"
                final_dict['depth_score'] = 0

        if pdf_dc['girdle'] == 'none' or pdf_dc['girdle'] == 'false' or pdf_dc['girdle'] == 'None':
            pdf_dc['girdle'] = "ND"

        if pdf_dc['girdle'] == "ND" or pdf_dc["girdle"] is None:
            final_dict['girdle_score'] = 0
        else:
            # girdle_point = check_girdle(gridle, girdle_gia, characteristic_data)
            girdle_point = assign_girdle_value(girdle_gia, characteristic_data)
            final_dict['girdle_score'] = girdle_point

        if pdf_dc['polish'] == "ND" or pdf_dc['polish'] is None:
            final_dict['polish_score'] = 0
        else:
            polish_point = check_polish("None", polish_value, characteristic_data)
            final_dict['polish_score'] = polish_point
        if pdf_dc['symmetry'] == "ND" or pdf_dc["symmetry"] is None:
            final_dict['symmetry_score'] = 0
        else:
            symmetry_point = check_symmetry('None', symmetry_value, characteristic_data)
            final_dict['symmetry_score'] = symmetry_point

        if pdf_dc['cut'] == "ND" or pdf_dc['cut'] is None:
            final_dict['cut_score'] = 0
        else:
            cut_grade_score = check_cut_grade('None', cut_grade_value, characteristic_data)
            final_dict['cut_score'] = cut_grade_score

        if pdf_dc['fluorescence'] is not None and pdf_dc['color_grade'] is not None:
            value = check_fluorescence(pdf_dc['fluorescence'], characteristic_data, pdf_dc['color_grade'])
            if value is None:

                final_dict['fluorescence_score'] = 0
            else:
                final_dict['fluorescence_score'] = value
        else:
            if affiliate_process_data is not None and affiliate_process_data:
                pdf_dc['fluorescence'] = affiliate_process_data['fluorescence']
                pdf_dc['color_grade'] = affiliate_process_data['color']
                if pdf_dc['fluorescence'] is not None and pdf_dc['color_grade'] is not None:
                    value = check_fluorescence(pdf_dc['fluorescence'], characteristic_data, pdf_dc['color_grade'])
                    if value is None:

                        final_dict['fluorescence_score'] = 0
                    else:
                        final_dict['fluorescence_score'] = value
                else:
                    final_dict['fluorescence_score'] = 0
                    if pdf_dc['fluorescence'] is None:
                        pdf_dc["fluorescence"] = 'ND'
                    if pdf_dc['color_grade'] is not None:
                        pdf_dc["color_grade"] = 'ND'
            else:
                final_dict['fluorescence_score'] = 0
                if pdf_dc['fluorescence'] is None:
                    pdf_dc["fluorescence"] = 'ND'
                if pdf_dc['color_grade'] is not None:
                    pdf_dc["color_grade"] = 'ND'

        if "key_to_symbol" in pdf_dc and pdf_dc['key_to_symbol'] is not None:
            symbol_value = check_symbol(pdf_dc['key_to_symbol'], characteristic_data)

            final_dict['key_to_symbol_score'] = symbol_value
        else:
            final_dict['key_to_symbol_score'] = 5

        total_marks = sum(list(final_dict.values()))
        overall_marks = (int(len(final_dict.values()))) * 5
        percent = (total_marks / overall_marks) * 100
        final_dict['digisation_score'] = f'{round(percent, 2)}%'
        logger.debug(
            "Round fetch_data_round completed for shape=%s with digisation_score=%s",
            pdf_dc.get("shape"),
            final_dict['digisation_score'],
        )
        score_dict = score_info_dict(final_dict)
        score_dict, pdf_dc = dict_np(score_dict, pdf_dc)

        return score_dict, pdf_dc_new, pdf_dc
    else:
        logger.warning("fetch_data_round: cannot determine shape from data, returning error status")
        return {"Status": "can't find shape from data"}, pdf_dc_new, pdf_dc


def dict_np(score_dict, pdf_dc):
    data_ls = ["cut_score", "crown_angle_score", "pavilion_height_score", "pavilion_angle_score", "measurement_score",
               "length_width_ratio_score"]
    for s in data_ls:
        if s in score_dict and score_dict[s] is None:
            score_dict[s] = "Not Applicable"
        elif s not in score_dict:
            score_dict[s] = "Not Applicable"
    pdf_ls = ["cut", "crown_angle", "pavilion_height", "pavilion_angle", "crown_height", "star_length",
              "lower_half_length"]
    for pdf_d in pdf_ls:
        if pdf_d in pdf_dc and pdf_dc[pdf_d] is None:
            pdf_dc[pdf_d] = "Not Applicable"
        elif pdf_d not in pdf_dc:
            pdf_dc[pdf_d] = "Not Applicable"
    return score_dict, pdf_dc


class OutputDictionary(TypedDict):
    cut_score: Optional[int]
    digisation_score: Optional[str]
    culet_score: Optional[int]
    depth_score: Optional[int]
    fluorescence_score: Optional[int]
    girdle_score: Optional[int]
    polish_score: Optional[int]
    symmetry_score: Optional[int]
    table_size_score: Optional[int]
    measurement_score: Optional[int]
    pavilion_angle_score: Optional[int]
    key_to_symbol_score: Optional[int]
    pavilion_height_score: Optional[int]
    length_width_ratio_score: Optional[int]
    crown_angle_score: Optional[int]


def score_info_dict(input_dict: dict) -> OutputDictionary:
    cut_score = input_dict.get('cut_score', None)
    digisation_score = input_dict.get('digisation_score', None)
    culet_score = input_dict.get('culet_score', None)
    depth_score = input_dict.get('depth_score', None)
    fluorescence_score = input_dict.get('fluorescence_score', None)
    girdle_score = input_dict.get('girdle_score', None)
    polish_score = input_dict.get('polish_score', None)
    symmetry_score = input_dict.get('symmetry_score', None)
    table_size_score = input_dict.get('table_size_score', None)
    measurement_score = input_dict.get('measurement_score', None)
    pavilion_angle_score = input_dict.get('pavilion_angle_score', None)
    key_to_symbol_score = input_dict.get('key_to_symbol_score', None)
    pavilion_height_score = input_dict.get('pavilion_height_score', None)
    length_width_ratio_score = input_dict.get('length_width_ratio_score', None)
    crown_angle_score = input_dict.get('crown_angle_score', None)

    return {'cut_score': cut_score, 'digisation_score': digisation_score, "pavilion_angle_score": pavilion_angle_score,
            "pavilion_height_score": pavilion_height_score, "measurement_score": measurement_score,
            "table_size_score": table_size_score, "symmetry_score": symmetry_score, "polish_score": polish_score,
            "girdle_score": girdle_score, "fluorescence_score": fluorescence_score, "depth_score": depth_score,
            "culet_score": culet_score, "key_to_symbol_score": key_to_symbol_score,
            'length_width_ratio_score': length_width_ratio_score, "crown_angle_score": crown_angle_score}

