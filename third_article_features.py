# Imports
import simpy
from scipy.stats import expon
import random
import numpy as np
import pandas as pd

# Global lists / DataFrames
stations_list = []
finished_orders = 0
early_orders = 0
earliness_list = []
tardy_orders = 0
tardiness_list = []

# Routing of the product types
routing = {1: [1, 2, 3], 2: [2, 3, 1], 3: [3, 2, 1], 4: [3, 1], 5: [2, 3]}

# Track order information
order_number = 0

# Simulation Parameters
period_length = 1440
period = 1
new_order_time = 80
SIM_TIME = 1000000
env = simpy.Environment()

# Order Pool
order_pool = []
order_pool_dict = dict()

# Order tracking
order_tracking_dict = dict()
order_tracking_df = pd.DataFrame()
order_features_df = pd.DataFrame()


# Tracking
def order_track_creation(order, environment):
    """
    Tracks the information of each order at the time it was created.
    :param order: The newly created order.
    :param environment: The orders' environment.
    :return: Appends the information to the ta.order_tracking_dict.
    """
    global order_tracking_dict
    global period

    order_tracking_dict[order.order_id] = dict()
    order_tracking_dict[order.order_id]['product_type'] = order.product_type
    order_tracking_dict[order.order_id]['due_date'] = order.due_date
    order_tracking_dict[order.order_id]['time_created'] = environment.now
    order_tracking_dict[order.order_id]['period_created'] = period


def order_track_release(order, environment):
    """
    Tracks the time the order was released.
    :param order: The released order.
    :param environment: The orders' environment.
    :return: Appends the information to the ta.order_tracking_dict.
    """
    global order_tracking_dict

    if order.order_id in order_tracking_dict.keys():
        order_tracking_dict[order.order_id]['time_released'] = environment.now


def order_track_finished(order_id, product_type, environment, station):
    """
    This function first checks if the order visited its last station (== the order is finished).
    If that is the case, the time the order was finished is calculated, and the sftt.
    :param order_id: The orders ID.
    :param product_type: The orders' product type.
    :param environment: The orders' environment.
    :param station: The station the order visited.
    :return: Appends the information to the ta.order_tracking_dict.
    """
    global order_tracking_dict
    global order_tracking_df

    if order_id in order_tracking_dict.keys():
        if station.number == routing.get(product_type)[-1]:
            order_tracking_dict[order_id]['time_finished'] = environment.now

            # Calculate SFTT
            time_released = order_tracking_dict[order_id]['time_released']
            order_tracking_dict[order_id]['sftt'] = environment.now - time_released

            # Store information in ta.order_tracking_df
            new_dict = order_tracking_dict.pop(order_id)
            new_dict['order_id'] = order_id
            new_df = pd.DataFrame(new_dict, index=['order_id'])
            new_df.index.names = ['order_id']
            order_tracking_df = pd.concat([order_tracking_df, new_df])


# Track features
def get_wip():
    """
    Calculated the WIP.
    :return: Returns the wip.
    """
    global stations_list

    wip = 0
    for station in stations_list:
        wip += len(station.machine.queue)

    wip += 3
    return wip


def nb_orders_queue_routing(product_type):
    """
    Calculates the number of orders waiting in front of the stations the order needs to visit.
    :param product_type: The orders' product type.
    :return: Return the number of orders waiting in front of the stations on the orders' routing.
    """
    global stations_list
    global routing

    nb_queue_routing = 0

    for station_nr in routing.get(product_type):
        nb_queue_routing += len(stations_list[station_nr - 1].machine.queue)

    return nb_queue_routing


def last_sftt(product_type):
    """
    Takes the most recently finished sftt of the oder with the same product type.
    :param product_type: The orders' product type.
    :return: Returns the sftt of the most recently finished order of the same product type.
    """
    global order_tracking_df
    try:
        last_sftt_1 = order_tracking_df['sftt'].loc[order_tracking_df['product_type'] == product_type].tail(1).item()
    except:
        last_sftt_1 = 0
    return last_sftt_1


def last_sftt_5(product_type):
    """
        Takes the most 5 recently finished sftt of the oder with the same product type.
        :param product_type: The orders' product type.
        :return: Returns the mean and median sftt of the 5 most recently finished order of the same product type.
        """
    global order_tracking_df
    try:
        last_5_sftt = order_tracking_df['sftt'].loc[order_tracking_df['product_type'] == product_type].tail(5)

        mean_last_sftt_5 = np.mean(last_5_sftt)
        median_last_sftt_5 = np.median(last_5_sftt)

    except:
        mean_last_sftt_5 = 0
        median_last_sftt_5 = 0

    return mean_last_sftt_5, median_last_sftt_5


def last_sftt_50(product_type):
    """
        Takes the most 5 recently finished sftt of the oder with the same product type.
        :param product_type: The orders' product type.
        :return: Returns the mean and median sftt of the 5 most recently finished order of the same product type.
        """
    global order_tracking_df

    try:
        last_50_sftt = order_tracking_df['sftt'].loc[order_tracking_df['product_type'] == product_type].tail(
            50)

        mean_last_sftt_50 = np.mean(last_50_sftt)
        median_last_sftt_50 = np.median(last_50_sftt)
    except:
        mean_last_sftt_50 = 0
        median_last_sftt_50 = 0
    return mean_last_sftt_50, median_last_sftt_50


def collect_features(order):
    """
    This functions calls all the follwing functions to collect the orders features.
    :param order: The new order.
    :return: Appends the information to the order_features_df
    """
    global order_features_df

    wip = get_wip()
    nb_orders_routing_queue = nb_orders_queue_routing(order.product_type)
    sftt_1 = last_sftt(order.product_type)
    sftt_5_mean, sftt_5_median = last_sftt_5(order.product_type)
    sftt_50_mean, sftt_50_median = last_sftt_50(order.product_type)

    new_df = pd.DataFrame({'order_id': order.order_id,
                           'wip': wip,
                           'nb_order_queue_routing': nb_orders_routing_queue,
                           'last_sftt': sftt_1,
                           'last_5_sftt_mean': sftt_5_mean,
                           'last_5_sftt_median': sftt_5_median,
                           'last_50_sftt_mean': sftt_50_mean,
                           'last_50_sftt_median': sftt_50_median,
                           }, index=['order_id'])

    order_features_df = pd.concat([order_features_df, new_df])


# Sorting definitions
def edd(order_pool):
    """
    This functions sorts the given order_pool by their earliest due date.
    :param order_pool: The current order pool.
    :return: Returns the list sorted by the earliest due date.
    """
    return sorted(order_pool, key=lambda x: x.due_date)


def earliest_prd(order_pool):
    """
    This function sorts the given order_pool by their earliest planned release date.
    :param order_pool: The current order pool.
    :return: Returns the list sorted by the earliest planned release date.
    """
    return sorted(order_pool, key=lambda x: x.prd)


# Predict SFTT
def expected_sftt(order):
    """
    This function calculates the expected mean SFTT for each order and subtracts it in the second step from
    the orders due date. To do this the number of stations on the orders routing is multiplied by the mean
    production time of each station (100).
    :param order: The current order.
    """
    global order_pool_dict

    order.prd = order.due_date - (len(routing.get(order.product_type)) * 100)
    release_period = int((order.prd) / 1440)

    if release_period in order_pool_dict.keys():
        order_pool_dict[release_period].append(order)
    else:
        order_pool_dict[release_period] = [order]


# BIL Release
def bil(order_pool_dict):
    """
    This function filters the order pool dict, that all orders with the referring period are released.
    :param order_pool_dict: The exisitng dict where all created orders are stored in.
    :return: A list of orders to be released.
    """
    global period
    try:
        release_list = []
        l = order_pool_dict.keys()
        o = period
        for periode in range(1, period + 1):
            if periode in order_pool_dict.keys():
                release_list.extend(order_pool_dict.get(periode))
                del order_pool_dict[periode]
    except:
        release_list = []

    return release_list


# IR Release
def ir(order_pool):
    """
    This functions takes the order_pool and clears it.
    :param order_pool: The current order pool
    :return: returns a list of orders to be released.
    """
    release_list = []
    release_list.extend(order_pool)
    order_pool.clear()

    return release_list


def track_order(due_date, product_type, station_number, time):
    """
    This function first checks if the order visited its last station (this means the order is finished) and later
    it checks whether the order missed its due date or was finished on time.
    :param due_date: The orders' due date.
    :param product_type: The orders' product type.
    :param station_number: The current station the order visited.
    :param time: The time the order leaved the station.
    """
    global finished_orders
    global early_orders
    global earliness_list
    global tardy_orders
    global tardiness_list

    if station_number == routing.get(product_type)[-1]:
        finished_orders += 1

        # If the order is finished before its due date it is an early order
        if time < due_date:
            early_orders += 1
            earliness = due_date - time
            earliness_list.append(earliness)
        else:
            tardy_orders += 1
            tardiness = due_date - time
            tardiness_list.append(tardiness)


class Order:
    """
    In this class first orders are generated. Afterwards sent to the stations on the routing where the order
    is handled. Also, the tracking for each order is done here.
    """
    global order_number
    global period_length
    global new_order_time
    global stations_list
    global order_pool
    global period

    def __init__(self, environment, order_id, product_type, due_date):
        """
        Here the variables for the orders are defined.
        :param environment: SimPy Environment()
        """
        self.env = environment
        self.order_id = order_id  # Identifier for each order
        self.product_type = product_type
        self.due_date = due_date
        self.prd = 0

    def handle_order(self, station):
        """
        The order request the station. Whether the order needs to wait or is immediately processed.
        :param station: The station for the order on the routing.
        """

        # Order requests the station
        with station.machine.request() as request:
            print(f"Order with order_id {self.order_id} arrives at station {station.number} at {self.env.now}")
            yield request
            # Get Processing time
            processing_time = expon.rvs(scale=100).round()
            # Use the station
            print(f"Order with order_id {self.order_id} is going to be processed at station {station.number} at "
                  f"{self.env.now} with processing time {processing_time}")
            yield self.env.timeout(processing_time)
            print(f"Order with order_id {self.order_id} leaves the station {station.number} at {self.env.now}")

            # Track orders, if finished
            track_order(self.due_date, self.product_type, station.number, self.env.now)
            order_track_finished(self.order_id, self.product_type, self.env, station)

    def get_station(self):
        """
        The next station on the product types routing is selected.
        :return: Sending the order to the next station (handle_order() ).
        """
        global stations_list

        # Get the orders stations
        stations = routing.get(self.product_type)

        # Iterate over each station
        for station in stations:
            # Send to the next station
            station = stations_list[station - 1]
            yield self.env.process(self.handle_order(station))

    def generate_orders(self):
        """
        In this function new orders are created. each order gets an order_id, then a random product type
        and the orders due date is calculated. A new order is created after the specified time above.
        """
        global order_number
        global period

        # Global order_id
        order_number += 1

        # Order attributes
        self.order_id = order_number
        self.product_type = random.randint(1, 5)
        self.due_date = self.env.now + (random.randint(2, 15) * period_length)

        # Create new Order
        order_new = Order(self.env, self.order_id, self.product_type, self.due_date)

        # Track order
        order_track_creation(order_new, self.env)
        collect_features(order_new)

        # Predict SFTT
        expected_sftt(order_new)

        # Append order to order_pool list
        order_pool.append(order_new)

        while True:
            yield self.env.timeout(new_order_time)

            # Increase order_id
            order_number += 1

            # Order attributes
            self.order_id = order_number
            self.product_type = random.randint(1, 5)
            self.due_date = self.env.now + (random.randint(2, 15) * period_length)

            # Create new order
            order_new = Order(self.env, self.order_id, self.product_type, self.due_date)

            # Track order
            order_track_creation(order_new, self.env)
            collect_features(order_new)

            # Predict SFTT
            expected_sftt(order_new)

            # Append order to order_pool list
            order_pool.append(order_new)

            if self.env.now >= period * period_length:
                # Increase period for periodic release
                period += 1

                for order_created in earliest_prd(bil(order_pool_dict)):
                    # Send order to the first stations
                    self.env.process(order_created.get_station())

                    # Track order release
                    order_track_release(order_created, self.env)


# Initialize the station class
class Station:
    """
    This class contains the stations used in the simulation.
    """

    def __init__(self, number, environment):
        self.env = environment
        self.number = number
        self.machine = simpy.Resource(environment, 1)


# Create 3 stations
station1 = Station(1, env)
station2 = Station(2, env)
station3 = Station(3, env)

# Append stations to the stations_list
stations_list.append(station1)
stations_list.append(station2)
stations_list.append(station3)

# Create instance of class Order
order = Order(env, 1, 1, 1)

env.process(order.generate_orders())

# Simulation RunTime
env.run(until=SIM_TIME)

# Print Performance
scenario = 'IR_EDD'
print(f"###{scenario}: In total {order_number} Orders were created.")
print(f"###{scenario}: {finished_orders} Orders were finished.")
print(f"###{scenario}: {early_orders} Orders were finished in time.")
print(f"###{scenario}: {tardy_orders} Orders were finished too late.")
mean_earliness = np.sum(earliness_list) / early_orders
mean_tardiness = np.sum(tardiness_list) / tardy_orders
print(f"###{scenario}: Mean earliness {mean_earliness}.")
print(f"###{scenario}: Mean tardiness {mean_tardiness}.")

l = order_tracking_df
f = order_features_df

final_df = order_tracking_df.merge(order_features_df, how='left', left_on=order_tracking_df['order_id'],
                                   right_on=order_features_df['order_id'])

final_df.to_csv('name.csv')
