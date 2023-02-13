# Imports
import simpy
from scipy.stats import expon
import random

# Global lists / DataFrames
stations_list = []
finished_orders = 0
early_orders = 0
tardy_orders = 0

# Routing of the product types
routing = {1: [1, 2, 3], 2: [2, 3, 1], 3: [3, 2, 1], 4: [3, 1], 5: [2, 3]}

# Track order information
order_number = 0

# Simulation Parameters
period_length = 1440
new_order_time = 80
SIM_TIME = 100000
env = simpy.Environment()


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
    global tardy_orders

    if station_number == routing.get(product_type)[-1]:
        finished_orders += 1

        # If the order is finished before its due date it is an early order
        if time < due_date:
            early_orders += 1
        else:
            tardy_orders += 1


class Order:
    """
    In this class first orders are generated. Afterwards sent to the stations on the routing where the order
    is handled. Also, the tracking for each order is done here.
    """
    global order_number
    global period_length
    global new_order_time
    global stations_list

    def __init__(self, environment, order_id, product_type, due_date):
        """
        Here the variables for the orders are defined.
        :param environment: SimPy Environment()
        """
        self.env = environment
        self.order_id = order_id  # Identifier for each order
        self.product_type = product_type
        self.due_date = due_date

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

        # Global order_id
        order_number += 1

        # Order attributes
        self.order_id = order_number
        self.product_type = random.randint(1, 5)
        self.due_date = self.env.now + (random.randint(2, 15) * period_length)

        # Create new Order
        order_new = Order(self.env, self.order_id, self.product_type, self.due_date)

        self.env.process(order_new.get_station())

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

            # Send order to the first stations
            self.env.process(order_new.get_station())


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
print(f"### In total {order_number} Orders were created.")
print(f"### {finished_orders} Orders were finished.")
print(f"### {early_orders} Orders were finished in time.")
print(f"### {tardy_orders} Orders were finished too late.")
