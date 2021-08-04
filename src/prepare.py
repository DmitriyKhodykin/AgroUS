"""
Data prepare module from USDA.

API methods: https://apps.fas.usda.gov/opendataweb/home
"""
import json
import pandas
import requests
import yaml

from auth import auth


def main():
    """
    Data prepare pipeline.
    """
    dataframe = CreateUsdaDataset()
    dataframe.save_dataframe()


class CreateUsdaDataset:

    def __init__(self):
        self.url = 'https://apps.fas.usda.gov/OpenData'
        self.headers_usda = {
            'Accept': 'application/json',
            'API_KEY': auth.usda_apikey
        }

    def save_dataframe(self):
        """
        Save dataframe with all commodities in project data dir.
        """
        dataframe = self.merge_dataset_and_catalogs()
        try:
            dataframe.to_parquet('data/data.parquet')
            print('All data saved...')
        except FileNotFoundError as e:
            print(e)

    def merge_dataset_and_catalogs(self) -> pandas.DataFrame:
        """
        Merge main dataset an catalogs with description of measures.
        :return: merged dataframe
        """
        dataframe = self.append_commodities_into_dataset()
        print('Get catalogs through API...')
        commodities = self._get_api_data_by_link(f'{self.url}/api/psd/commodities')
        print(commodities.head())
        attributes = self._get_api_data_by_link(f'{self.url}/api/psd/commodityAttributes')
        print(attributes.head())
        units = self._get_api_data_by_link(f'{self.url}/api/psd/unitsOfMeasure')
        print(units.head())
        print('Merge main dataset and catalogs with features key...')
        dataframe = dataframe.merge(
            commodities, how='left', on='commodityCode'
        ).merge(
            attributes, how='left', on='attributeId'
        ).merge(
            units, how='left', on='unitId'
        )
        print(dataframe.head())
        print('About merged dataframe:')
        print(dataframe.info())
        return dataframe

    def append_commodities_into_dataset(self) -> pandas.DataFrame:
        """
        Append commodity data into common dataframe.
        :return: dataframe with commodities data
        """
        dataframe = pandas.DataFrame()
        print('Get all possible commodity codes...')
        codes = self._get_api_data_by_link(f'{self.url}/api/psd/commodities')
        commodity_codes = list(codes['commodityCode'])
        # Reporting period
        period_start = yaml.safe_load(open('params.yaml'))['period']['start']
        period_stop = yaml.safe_load(open('params.yaml'))['period']['stop']
        print(f'Reporting period from: {period_start} to: {period_stop} ')

        print('Iteration by commodity codes and periods...')
        for code in commodity_codes:
            for year in range(period_start, period_stop, 1):
                tmp_data = self.get_data_by_commodity_and_year(code, year)
                dataframe = dataframe.append(tmp_data)
        print(dataframe.head())
        print('About appended dataframe:')
        print(dataframe.info())
        return dataframe

    def get_data_by_commodity_and_year(self, commodity_code: str,
                                       year: int) -> pandas.DataFrame:
        """
        Get data by commodity code.
        :return: dataframe with commodity data
        """
        address = f'/api/psd/commodity/{commodity_code}' \
                  f'/country/all/year/{year}'
        request = requests.get(f'{self.url}{address}',
                               headers=self.headers_usda)
        response = json.loads(request.text)
        commodity = pandas.DataFrame(response)
        return commodity

    def _get_api_data_by_link(self, link: str) -> pandas.DataFrame:
        """
        Import data from usda API.
        :return: dataframe with data
        """
        try:
            request = requests.get(link, headers=self.headers_usda)
            response = json.loads(request.text)
            data = pandas.DataFrame(response)
            return data
        except json.decoder.JSONDecodeError as e:
            print(e)


if __name__ == '__main__':
    main()
