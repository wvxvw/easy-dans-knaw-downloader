# -*- coding: utf-8 -*-

import queue
import time
import logging
import json

from argparse import ArgumentParser
from multiprocessing import Process, Queue, Event

from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains


# TODO(wvxvw): Also configure logging
parser = ArgumentParser('easy.dans.knaw.nl downloader')
parser.add_argument(
    '-o',
    '--output',
    default=None,
    help='''
    Store scrapper's output in this directory
    (default: current directory)
    ''',
)
parser.add_argument(
    '-n',
    '--node',
    action='append',
    default=[],
    help='''
    Selenium worker node URL
    ''',
)
parser.add_argument(
    '-v',
    '--verbosity',
    default=logging.WARNING,
    type=int,
    help='''
    Logging verbosity level. Default 30 (WARNING).
    ''',
)
parser.add_argument(
    '-d',
    '--dataset',
    help='''
    Dataset id to download (appears in the URL of the site):
    Example: 
    https://easy.dans.knaw.nl/ui/datasets/id/easy-dataset:112935
    112935 is the id.
    ''',
)


class WebDriverProcess(Process):

    def __init__(
            self,
            node,
            sink,
            jobs,
            is_done,
            out_dir,
            url,
            timeout=1,
    ):
        super().__init__()
        self.node = node
        self.node_url = 'http://{}:5555/wd/hub'.format(node)
        self.sink = sink
        self.jobs = jobs
        self.out_dir = out_dir
        self.url = url
        self.timeout = timeout

    def run(self):
        options = webdriver.ChromeOptions()
        prefs = {'download.default_directory': self.out_dir}
        options.add_experimental_option('prefs', prefs)
        self.driver = webdriver.Remote(
            command_executor=self.node_url,
            desired_capabilities=DesiredCapabilities.CHROME,
            chrome_options=options,
        )
        self.driver.get(self.url)
        self.driver.maximize_window()
        while True:
            try:
                tx = self.jobs.get_nowait()
                self.process_item(tx)
            except queue.Empty:
                time.sleep(self.timeout)

    def process_item(self, item):
        logging.info('Processing: {}'.format(item))
        response = self.download_item(item)
        logging.info('Downloaded {}'.format(item))
        self.sink.put((item, self.node, response))

    def download_item(self):
        return None
        # extras_path = (
        #     '/html/body/div[1]/div[3]/div/div[3]/div/div[3]/'
        #     'div[2]/div[1]/div[2]/div/div[2]/a'
        # )
        # expand = self.driver.find_elements_by_xpath(extras_path)
        # if expand:
        #     message = expand[0].text
        #     remaining = int(message.split()[-2][1:])
        #     logging.info('Expanding: {}'.format(message))
        #     elt = WebDriverWait(self.driver, 10).until(
        #         EC.element_to_be_clickable(
        #             (By.XPATH, extras_path),
        #         ))
        #     actions = ActionChains(self.driver)
        #     desired_y = (elt.size['height'] / 2) + elt.location['y']
        #     middle = self.driver.execute_script('return window.innerHeight') / 2
        #     current_y = middle + self.driver.execute_script('return window.pageYOffset')
        #     scroll_y_by = desired_y - current_y
        #     self.driver.execute_script('window.scrollBy(0, arguments[0]);', scroll_y_by)
        #     actions.click(elt).perform()
        #     remaining -= 10
        #     if remaining > 0:
        #         WebDriverWait(self.driver, 10).until(
        #             EC.text_to_be_present_in_element(
        #                 (By.XPATH, extras_path),
        #                 'Load more inputs... ({} remaining)'.format(remaining),
        #             ))


def scrap(argsv):
    pargs = parser.parse_args(argsv)

    logging.basicConfig(
        force=True,
        level=pargs.verbosity,
    )

    sink = Queue()
    jobs = Queue()
    workers = {
        node: WebDriverProcess(
            node,
            sink,
            jobs,
            (
                'https://easy.dans.knaw.nl/ui/datasets/'
                'id/easy-dataset:{}/tab/2'.format(pargs.dataset)
            ),
            pargs.output,
        )
        for node in pargs.node
    }

    for worker in workers.values():
        worker.start()

    try:
        dfile = 0
        jobs.put_nowait(dfile)

        while workers:
            try:
                item, node, response = sink.get()

                if not response:
                    workers[node].terminate()
                    del workers[node]
                else:
                    jobs.put_nowait(dfile)
            except queue.Empty:
                time.sleep(1)

        logging.info('Finished downloading.')
    finally:
        for worker in workers.values():
            worker.terminate()


def main(argsv):
    try:
        scrap(argsv)
    # TODO(wvxvw): This needs to be more specific about errors
    except Exception as e:
        logging.exception(e)
        return 1
    return 0
