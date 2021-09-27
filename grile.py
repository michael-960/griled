import ujson
import socket
import pathlib
import __main__
import os
import shutil
import datetime
from pprint import pprint
from .temp_man import TempFileHandler


_DATE_UPDATED = "2021/9/26"
_VERSION = "1.1"

class Grile:
    def __init__(self, file_path):
        with open(file_path, 'r') as f:
            d = ujson.load(f)

        self.machines = dict()
        for name in d['machines']:
            info = d['machines'][name]
            machine = Machine(info)
            self.machines[name] = machine


        self.yell(f'Version {_VERSION} last updated on {_DATE_UPDATED}')


    def at_server(self):
        hostname = socket.gethostname()
        for mach in self.machines:
            machine = self.machines[mach]
            if machine.name == hostname:
                return True
        return False

    def get_current_machine(self):
        hostname = socket.gethostname()
        if self.at_server():
            for mach in self.machines:
                machine = self.machines[mach]
                if machine.name == hostname:
                    return machine
        self.yell('Client is not a valid machine')
        return None

    def get_data_dir(self):
        if self.at_server():
            return self.get_current_machine().datadir
        return None

    def create_simulation(self):
        if self.at_server():
            mainpath = pathlib.Path(__main__.__file__).absolute()
            info_file = str(mainpath.parent.absolute()) + '/info.json'
            self.yell(f'Simulation created using {info_file}')
            with open(info_file, 'r') as f:
                info = ujson.load(f)

            sim = Simulation(info, self)
            simdir = self.get_path(self.get_current_machine().name, sim.group, sim.name)

            if not os.path.exists(simdir):
                self.yell(f'{simdir} does not exist. Directory created')
                os.makedirs(simdir)
            
                        
            logfile = simdir + '/log.json'
            if not os.path.exists(logfile):
                with open(logfile, 'w+') as f:
                    ujson.dump([], f, indent=4)
                self.yell(f'Created log file at {logfile}')

            return sim

        else:
            self.yell('Not at server')

        return None


    def finalize(self, sim):
        simdir = self.get_path(self.get_current_machine().name, sim.group, sim.name)
        existing = os.listdir(simdir)
        sim_id = 0
        while True:
            if str(sim_id) not in existing:
                break
            sim_id += 1
        self.yell(f'This simulation has been {sim.group}:{sim.name}:{sim_id}')
        simiddir = simdir + f'/{sim_id}'

        if not os.path.exists(simiddir):
            os.makedirs(simiddir)
            self.yell(f'{simiddir} does not exist. Directory created')
        else:
            self.yell(f'{simiddir} already exists. Contents will be overridden')

        for key in sim.key_files:
            if key in sim.data_content:
                cont = sim.data_content[key]
                filename = sim.key_files[key]["file"]
                filepath = f'{sim.group}/{sim.name}/{sim_id}/{filename}'
                self.write_file(filepath, cont['content'], cont['is_json'])
                self.yell(f'Data written to {filepath} using key {key}')

        sim.log['id'] = sim_id
        log = self.retrieve_log(self.get_current_machine().name, sim.group, sim.name)
        log.append(sim.log)
        self.write_log(self.get_current_machine().name, sim.group, sim.name, log)


    def write_file(self, path, content, is_json=False):
        if self.at_server():
            filepath = self.get_data_root() + f'/{path}'
        with open(filepath, 'w+') as f:
            if is_json:
                ujson.dump(content, f, indent=4)
            else:
                f.write(content)

    #2021 0902
    def transfer_file(self, key, src_path, delete=False):
        if not self.ended:
            self.yell('Please end the simulation before writing any files.')
        if self.at_server() and (not self.simulation is None):
            if key in self.simulation.data:
                filename = self.simulation.data[key]['file']
                dest_path = self.get_data_dir() + f'/{self.date}/{self.simulation.name}/{self.id}/{filename}'
                shutil.copy(src_path, dest_path)
                self.yell(f'Data transferred to {dest_path} from {src_path} using key {key}')

                if delete:
                    os.remove(src_path)
                    self.yell(f'File {src_path} removed')
            else:
                self.yell(f'{key} is not an allowed key')


    def retrieve_data(self, machine, group, simulation, sid, filename, is_json=False):
        loc_machine = socket.gethostname()

        if self.at_server():   
            filepath = None
            if socket.gethostname() == machine:
                filepath = \
                self.get_path(machine, group, simulation, sid, filename)
            else:
                #TODO implement common dirs
                machine1 = self.machines[loc_machine]
                machine2 = self.machines[machine]
                
                if machine1.share and machine2.share:
                    prefix = machine1.common.replace('$machine$',
                        f'{machine2.name}.data')
                    print(f'prefix={prefix}')
                    filepath = f'{prefix}/{partialpath}'
                    
        
            with open(filepath, 'r') as f:
                    if is_json:
                        return ujson.load(f)
                    else:
                        return f.read()
                           
        else:
            #TODO implement remote dirs
            return None


    def write_log(self, machine, group, simulation, log):
        if self.at_server():
            logpath = self.get_path(machine, group, simulation) + '/log.json'
            with open(logpath, 'w+') as f:
                ujson.dump(log, f, indent=4)
        else:
            self.yell('Cannot write log at client')

    def retrieve_log(self, machine, group, simulation):
        if self.at_server():   
            logpath = self.get_path(machine, group, simulation) + '/log.json'

            with open(logpath, 'r') as f:
                return ujson.load(f)
        else:
            self.yell('Cannot retrieve log from client')
            return None

    def delete(self, machine, group, simulation, sim_id):
        simpath = \
        f'{group}/{simulation}'

        if self.at_server():
            if socket.gethostname() != machine:
                self.yell('Unable to delete file from remote server')
                return
            else:
                log = self.retrieve_log(machine, group, simulation)
                found = False
                for i in range(len(log)):
                    sim = log[i]
                    if sim['id'] == sim_id:
                        found = True
                        self.yell(f'Found simulation {group}:{simulation}:{sim_id}. Summary:') 
                        print('------------------------------------------------------------')
                        pprint(sim)
                        print('------------------------------------------------------------')
                        confirm = self.ask(f'Remove simulation {group}:{simulation}:{sim_id} y/[n]? ')
                        
                        if confirm:
                            #sim_dir = self.machines[machine].datadir + '/' + simpath + f'/{sim_id}'
                            sim_dir = self.get_path(machine, group, simulation, sim_id)
                            hidden_dir = self.get_path(machine, group, simulation, f'.{sim_id}')
                            confirm = self.ask(f'Delete directory {sim_dir} y/[n]? ') 
                            if confirm:
                                del log[i]
                                log_path = self.get_path(machine, group, simulation) + '/log.json'
                                with open(log_path, 'w+') as f:
                                    ujson.dump(log, f, indent=4)

                                if os.path.isdir(hidden_dir):
                                    shutil.rmtree(hidden_dir)
                                    self.yell(f'Removed {hidden_dir}')

                                os.rename(sim_dir, hidden_dir)
                                self.yell('Updated log file {log_path}')
                                self.yell(f'{sim_dir} -> {hidden_dir}')

                            else:
                                pass
                        else:
                            pass
                        break

                if not found:
                    self.yell(f'Simulation {simulation}:{sim_id} not found.')

            

    def get_path(self, machine, group='', simulation='', sim_id='', filename=''):

        if not machine in self.machines:
            self.yell(f'{machine} is not in the list of registered servers')
            return None

        path = ''
        if self.get_current_machine().name == machine:
            path = self.machines[machine].datadir
        else: #share
            path = self.machines[machine].common

        if group == '':
            return path
        elif simulation == '':
            return f'{path}/{group}'
        elif sim_id == '':
            return f'{path}/{group}/{simulation}'
        elif filename == '':
            return f'{path}/{group}/{simulation}/{sim_id}'
        else:
            return f'{path}/{group}/{simulation}/{sim_id}/{filename}'

    
    def get_abstract_path_root(self, prefix):
        if self.at_server():
            machine = self.get_current_machine()
            if prefix == 'data':
                return machine.datadir
            if prefix == 'tmp':
                return machine.tmpdir

            self.yell(f'{prefix} is not a valid prefix')

        self.yell('At client')
        return None

    def get_data_root(self):
        return self.get_abstract_path_root('data')

    def get_fullpath(self, prefix, path):
        return self.get_abstract_path_root(prefix) + '/' + path

    def read(self, prefix, path):
        fullpath = self.get_fullpath(prefix, path)
        with open(fullpath, 'r') as f:
            s = f.read()
        return s

    def write(self, prefix, path, content):
        fullpath = self.get_fullpath(prefix, path)
        with open(fullpath, 'w+') as f:
            f.write(content)

    def remove(self, prefix, path):
        fullpath = self.get_fullpath(prefix, path)
        os.remove(fullpath)


    def yell(self, s):
        print(f'[Grile] {s}')

    def ask(self, s):
        return input(f'[Grile] {s}')
    

class Machine:
    def __init__(self, info):
        
        self.name = info['name']
        self.datadir = info['data']
        self.share = info['share']

        if self.share:
            self.common = info['common']
        self.tmpdir = info['tmp']      



class Simulation:
    
    def __init__(self, info, grile):
        self.name = info['name']
        self.description = info['description']
        self.params = info['params']
        self.key_files = info['data']
        self.group = info['group']
        self.log = dict()
        self.starttime = None
        self.grile = grile
        self.temphandler = TempFileHandler(self.grile)
        self.data_content = dict()

        self.ended = False
        self.stopped = False
        
    def start(self):
        tme = datetime.datetime.now()
        self.starttime = tme
        self.log['date'] = f'{tme.year}-{tme.month}-{tme.day}'
        self.log['time'] = f'{tme.hour}-{tme.minute}-{tme.second}'
        self.yell(f'Simulation started at {tme.year}-{tme.month}-{tme.day} {tme.hour}:{tme.minute}:{tme.second}')

    def stop(self):
        if not self.starttime is None:
            tme = datetime.datetime.now()
            self.log['duration'] = (tme - self.starttime).seconds
            self.stopped = True
            self.yell(f'Simulation stopped at {tme.year}-{tme.month}-{tme.day} {tme.hour}:{tme.minute}:{tme.second}')

        else:
            self.yell('Start the simulation before stopping it')

    def extra_info(self, info):
        if not self.ended:
            self.log['extra_info'] = info
        else:
            self.yell('This simulation has already been ended, log can no longer be modified')


    def put_content(self, key, content, is_json=False):
        if self.ended:
            self.yell('This simulation has already been ended, data content can no longer be modified')
            return

        if not self.stopped:
            self.yell('The simulation needs to be stopped before recording data')
            return

        if key in self.key_files:
            self.data_content[key] = {'content': content, 'is_json': is_json}
        else:
            self.grile.yell(f'{key} is not a valid key registered for {self.name}')


    def end(self):
        if not self.stopped:
            self.yell('Please stop the simulation before finalizing')
            return

        self.ended = True
        self.grile.finalize(self)

        
    def record_param(self, params):
        self.log['params'] = params    


    def yell(self, s):
        print(f'[Grile][{self.group}:{self.name}] {s}')

