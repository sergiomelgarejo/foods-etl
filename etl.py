#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np
import requests
import os


PATH = "https://api.nal.usda.gov/fdc/v1/foods/search"
API_KEY = os.environ.get('USDA_API_KEY')
MAX_RESULTS = 100

def get_totalpage():
    """
    Returns the number of pages.
    """
    try:
        url = f"{PATH}?api_key={API_KEY}&pageSize={MAX_RESULTS}"
        r = requests.get(url)
        r.raise_for_status()
        return r.json()['totalPages']
    except Exception as e:
        raise Exception(f'Request error! {e}')


def get_foods_pp(page):
    """
    Returns a list of foods per API page.
    """
    try:
        url = f"{PATH}?api_key={API_KEY}&pageNumber={page}&pageSize={MAX_RESULTS}"
        r = requests.get(url)
        r.raise_for_status()
        data = r.json()
        foods = data['foods']
        return foods
    except Exception as e:
        raise Exception(f'Request error! {e}')


def extract():
    """
    Returns a multi-dimentional array of all foods data requested to the API.
    """
    pages = get_totalpage()
    foods_list = []

    # for page in range(1, pages): !!!! uncomment in production
    for page in range(1, 3):
        foods = get_foods_pp(page)
        foods_list.append(foods)

    return foods_list


def tranform_foods_list(foods):
    """
    Join a sequence of foods arrays. Returns a one-dimention list.
    """
    foods_ = np.array(foods)
    return np.concatenate(foods_).tolist()


def t_foods(food):
    """
    Validates that the number of necessary fields of the Food object remains constant even if one or more fields are missing or unknown.
    """
    keys = [
        'fdcId', 'description', 'commonNames', 'foodCode', 
        'publishedDate', 'foodCategory', 'foodCategoryId'
    ]

    return {key: food.get(key, None) for key in keys}


def t_nutrients(food):
    """
    Validates that the number of necessary fields of the Nutrient object remains constant even if one or more fields are missing or unknown.
    """
    keys = [
        'nutrientId', 'nutrientName', 'nutrientNumber', 'unitName',
        'value', 'rank', 'indentLevel', 'foodNutrientId'
    ]

    return {key: food.get(key, None) for key in keys}


def transform(data):
    """
    Creates two DataFrames, one for foods object and another for the nutrients object.
    Also it creates the relationship between the foods and nutrients objects by 'fdcId'.
    """
    foods = tranform_foods_list(data)

    data_foods = []
    data_nutrients = []
    data_foodnutrients = []

    for food in foods:
        ft = t_foods(food)
        data_foods.append(ft)

        nutrients = food['foodNutrients']
        for nutrient in nutrients:
            nt = t_nutrients(nutrient)
            nt['fdcId'] = ft['fdcId']
            data_nutrients.append(nt) 

        df_foodnutrients = pd.DataFrame(data=data_nutrients)
        data_foodnutrients.append(df_foodnutrients)

    df_foods = pd.DataFrame(data=data_foods)
    df_nutrients = pd.concat(data_foodnutrients, ignore_index=True)

    return df_foods, df_nutrients


def main():
    data = extract()
    processed_data = transform(data)
    # load(processed_data)
    print(processed_data[0].shape)


if __name__ == '__main__':
    main()

