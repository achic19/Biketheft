import calendar
from word2number import w2n
import os, re
from pandas import DataFrame,Series

# globe parameters
months = list(map(lambda x: x.lower(), calendar.month_name))[1:]
project_folder = os.path.dirname(os.path.dirname(os.path.dirname(os.getcwd())))
period_time = {'days': 1, 'day': 1, 'weeks': 7, 'weeke': 7, 'week': 7, 'months': 30, 'month': 30, 'mo': 30,
               'years': 365, 'year': 365, 'yrs': 365}
not_found_list = ["not recovered", 'not recover', 'never', "wasn't recovered",
                  'ongoing', 'unrecovered', 'not been', 'still', 'no recovery',
                  'yet']


class InterpretTime:
    def __init__(self, gb_data):
        """
        The class is initialized for all the data and the methods update the new fields and tables
        :param gb_data:
        :return:
        """
        self.clean_dict = {}
        self.ref_key = 'num_of_days'
        self.new_gb_data = DataFrame(gb_data)
        self.field_to_work_on = 'count'
        self.new_gb_data.rename(columns ={'index':self.field_to_work_on},inplace=True)
        self.new_gb_data[self.ref_key] = ''
        self.res = Series()
        self.period = ''

    def update_dict(self, time):
        # add to the time takes to find bike dic the number of people (-1 is never)
        if time in self.clean_dict:
            self.clean_dict[time] += self.res[self.field_to_work_on]
        else:
            self.clean_dict[time] = self.res[self.field_to_work_on]
        self.new_gb_data[self.ref_key][self.period] = time

    @staticmethod
    def clean(my_str: str) -> str:
        """
        Clean unnecessary marks
        :param my_str: string to clean
        :return:
        """
        return my_str.rstrip('+').lstrip('(').lstrip('~').lstrip('>').rstrip('?').rstrip('.').rstrip(')')

    @staticmethod
    def test_dash(num: str):
        """
        average two values that are separated by dash
        :param num:
        :return:
        """
        if '-' in num:
            # for example 1-2 weeks
            nums_list = num.split('-')
            return (int(nums_list[0]) + int(nums_list[-1])) / 2
        else:
            return num

    @staticmethod
    def get_number_from_periods(temp_str: str, new_str: str) -> (str, str):
        """
        this function get string that include numeric and non-numeric values
        :param temp_str: the remaining part of the string
        :param new_str: the new numeric value_check to build
        :return:
        """
        is_digit = temp_str[0]
        if is_digit.isdigit():
            new_str = new_str + is_digit
            new_str, temp_str = InterpretTime.get_number_from_periods(temp_str[1:], new_str)
            return new_str, temp_str
        return new_str, temp_str

    def is_one(self, number: str):
        """
        this methed work on one string (following the splitting)
        :param number: an apparently single string
        :return:
        """
        is_digit = number[0]
        # for cases like 2month
        if is_digit.isdigit() and not number[-1].isdigit():
            new_str, period_str = InterpretTime.get_number_from_periods(number[1:], is_digit)
            temp_list = [new_str, period_str]
            return len(temp_list), new_str, temp_list

        else:
            number = InterpretTime.test_dash(number)
            self.update_dict(int(number))
            return -1, -1, -1

    # get period time (based on period_time dictionary)
    def more_than_one(self, num, date: str):
        factor = period_time[date]
        try:
            if isinstance(num, str):
                # num can be float or int
                if num == 'couple':
                    num = 2
                elif num == 'there':
                    num = 3
                else:
                    num = InterpretTime.test_dash(num)
            self.update_dict(int(float(num) * factor))
        except ValueError:
            # when num is string
            try:
                self.update_dict(int(w2n.word_to_num(num) * factor))
            except ValueError:
                # when the sting is not number
                self.update_dict(factor)

    # Find the first period mentioned in the temp_list
    def more_than_two_period_time(self, temp_list: list) -> bool:
        """
        :param temp_list: list of words that constitute the time
        :return:
        """
        # Find the first period mentioned in the temp_list
        periods = []
        for temp in temp_list:
            # clean the sting before searching
            new_key = InterpretTime.clean(temp)
            if new_key in period_time.keys():
                periods.append([new_key, temp_list.index(temp)])
        if len(periods) == 0:
            return False
        if len(periods) == 1:
            key_new = periods[0][0]
            my_index = periods[0][1]
            number_temp = temp_list[my_index - 1]
            # in case of for example 2 to/or 3 weeks
            conca = temp_list[my_index - 2]
            if (conca == 'to' or conca == 'or') and temp_list[my_index - 3].isdigit():
                number_temp = (int(number_temp) + int(temp_list[my_index - 3])) / 2
        else:
            sorty = sorted(periods, key=lambda item_temp: item_temp[1])
            key_new = sorty[0][0]
            my_index = sorty[0][1]
            number_temp = temp_list[my_index - 1]
            if temp_list[my_index + 1].lower() == 'and':
                # In case there is two time periods to ues
                key2 = sorty[1][0]
                val1 = int(number_temp) * period_time[key_new]
                val2 = int(temp_list[sorty[1][1] - 1]) * period_time[key2]
                self.update_dict(int(val1 + val2))
                return True
        self.more_than_one(number_temp, key_new)
        return True

    def more_than_two_month_name(self, my_string):
        # function when the participant mention month name
        flag = False  # in case no month name is found in the string
        number_list = []
        for month in months:
            if month in my_string:
                for item_my_string in my_string:
                    # Go over the sting to find the dates
                    if item_my_string == month:
                        continue
                    else:
                        try:
                            number_list.append(int(item_my_string[:-2]))
                        except ValueError:
                            continue
                self.update_dict(int(abs(number_list[1] - number_list[0])))
                flag = True
                break
        return flag

    def edge_case(self, temp_list: list, period_lower: str) -> bool:
        """
        look on some cases when no advanced processing is required
        :param temp_list: the string as list of substrings
        :param period_lower:  the original string
        :return:
        """
        if '0' in temp_list:
            self.update_dict(0)
            return True
        # for very specific case
        if 'not recovered-' in period_lower:
            self.update_dict(-1)
            return True
        if 'few' in period_lower or 'several' in period_lower:
            # can't use that time period
            return True
        if (any(item in period_lower for item in not_found_list)) and not any(
                self.clean(item) in period_time for item in temp_list):
            # That means not recovered yet
            self.update_dict(-1)
            return True

    def main_part(self, row):
        self.period = row[0]
        self.res = row[1]
        period_lower = self.period.lower()
        temp_list = re.split(' |/', period_lower)
        if self.edge_case(temp_list, period_lower):
            return
        number = InterpretTime.clean(temp_list[0])
        leny = len(temp_list)
        if leny == 1:
            leny, number, temp_list = self.is_one(number)
            if leny == -1:
                # no more edit is required
                return
        if leny == 2:
            temp_list[1] = InterpretTime.clean(temp_list[1])
            if temp_list[1] in period_time.keys():
                self.more_than_one(number, temp_list[1].lstrip())
            else:
                self.update_dict(int(temp_list[1]))
            return
        else:
            # clean unnecessary points from the string
            # temp_list = is_point_decimal_number(temp_list)
            if not self.more_than_two_period_time(temp_list):
                if not self.more_than_two_month_name(temp_list):
                    # looking for all the values in the string and calculate the average
                    list_cal = [int(is_day) for is_day in temp_list if is_day.isdigit()]
                    if len(list_cal) > 0:
                        self.update_dict(int(sum(list_cal) / len(list_cal)))
