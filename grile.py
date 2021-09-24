import ujson
import socket
import pathlib
import __main__
import os
import shutil
import datetime
from pprint import pprint


# updated 2021/9/22
__DATE_UPDATED = "2021/9/22"
__VERSION = "1.1"

class Grile:
    def __init__(self, file_path):
        with open(file_path, 'r') as f:
            d = ujson.load(f)

        self.machines = dict()
        for name in d['machines']:
            info = d['machines'][name]
            machine = Machine(info)
            self.machines[name] = machine


        self.yell(f'Version {__VERSION} last updated on {__DATE_UPDATED}')


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
        return None

    def get_data_dir(self):
        if self.at_server():
            return self.get_current_machine().datadir
        return None

    def create_simulation(self):
        if self.at_server():
            mainpath = pathlib.Path(__main__.__file__).absolute()
            info_file = str(mainpath.parent.absolute()) + '/info.json'
            with open(info_file, 'r') as f:
                info = ujson.load(f)

            sim = Simulation(info, self)
            simdir = self.get_data_dir() + f'/{self.date}/{sim.name}'

            if not os.path.exists(simdir):
                self.yell(f'{simdir} does not exist. Directory created.')
                os.makedirs(simdir)
            
                        
            logfile = self.get_data_dir() + f'/{self.date}/{sim.name}/log.json'
            if not os.path.exists(logfile):
                with open(logfile, 'w+') as f:
                    ujson.dump([], f, indent=4)
                self.yell(f'Created log file at {logfile}')

            return sim

        else:
            self.yell('Not at server')

            

    def end(self):
        if not self.starttime is None:
            tme = datetime.datetime.now()
            self.log['duration'] = (tme - self.starttime).seconds
        self.ended = True

        simdir = self.get_data_dir() + f'/{self.date}/{self.simulation.name}'
        existing = os.listdir(simdir)
        self.id = 0
        while True:
            if str(self.id) not in existing:
                break
            self.id += 1
        self.yell(f'This simulation has been {self.date}:{self.simulation.name}:{self.id}')
        simiddir = simdir + f'/{self.id}'

        if not os.path.exists(simiddir):
            self.yell(f'{simiddir} does not exist. Directory created')
            os.makedirs(simiddir)
        else:
            self.yell('{simiddir} already exists. Content will be overridden')
                

        self.log['id'] = self.id
        self.write_log()

    def record_param(self, params):
        self.log['params'] = params    

    def write_file(self, key, content, is_json=False):
        if not self.ended:
            self.yell('Please end the simulation before writing any files.')
            return
        if self.at_server() and (not self.simulation is None):
            if key in self.simulation.data:
                filename = self.simulation.data[key]['file']
                filepath = self.get_data_dir() + f'/{self.date}/{self.simulation.name}/{self.id}/{filename}'
                with open(filepath, 'w+') as f:
                    if is_json:
                        ujson.dump(content, f, indent=4)
                    else:
                        f.write(content)
                self.yell(f'Data written to {filepath} using key {key}')
            else:
                self.yell(f'{key} is not an allowed key.')

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


    def write_log(self, sim, sim_id):
        if self.at_server() and (not sim is None):
            logfile = self.get_data_dir() + f'/{sim.group}/{sim.name}/log.json'
            with open(logfile, 'r') as f:
                log = ujson.load(f)

            log.append(sim.log)
            with open(logfile, 'w+') as f:
                ujson.dump(log, f, indent=4)
    
    def retrieve_data(self, machine, date, simulation, sid, filename, is_json=False):
        partialpath = \
        f'{date}/{simulation}/{sid}/{filename}'
        loc_machine = socket.gethostname()


        if self.at_server():   
            filepath = None
            if socket.gethostname() == machine:
                filepath = \
                f'{self.machines[machine].datadir}/{partialpath}'
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

    def retrieve_log(self, machine, date, simulation):
        partialpath = \
        f'{date}/{simulation}/log.json'
        loc_machine = socket.gethostname()

        if self.at_server():   
            filepath = None
            if socket.gethostname() == machine:
                filepath = \
                self.get_path(machine, date, simulation) + '/log.json'
                f'{self.machines[machine].datadir}/{partialpath}'
            else:
                #TODO implement common dirs
                machine1 = self.machines[loc_machine]
                machine2 = self.machines[machine]
                
                if machine1.share and machine2.share:
                    prefix = machine1.common.replace('$machine$',
                        f'{machine2.name}.data')
                    self.yell(f'prefix={prefix}')
                    filepath = f'{prefix}/{partialpath}'
        

            with open(filepath, 'r') as f:
                return ujson.load(f)

        else:
            #TODO implement remote dirs
            return None

    def delete(self, machine, date, simulation, sim_id):
        simpath = \
        f'{date}/{simulation}'

        if self.at_server():
            if socket.gethostname() != machine:
                self.yell('Unable to delete file from remote server')
                return
            else:
                log = self.retrieve_log(machine, date, simulation)
                found = False
                for i in range(len(log)):
                    sim = log[i]
                    if sim['id'] == sim_id:
                        found = True
                        self.yell(f'Found simulation {simulation}:{sim_id}. Summary:') 
                        print('------------------------------------------------------------')
                        pprint(sim)
                        print('------------------------------------------------------------')
                        confirm = self.yell(f'Remove simulation {simulation}:{sim_id} y/[n]? ')
                        
                        if confirm:
                            #sim_dir = self.machines[machine].datadir + '/' + simpath + f'/{sim_id}'
                            sim_dir = self.get_path(machine, date, simulation, sim_id)
                            hidden_dir = self.get_path(machine, date, simulation, f'.{sim_id}')
                            confirm = self.yell(f'Delete directory {sim_dir} y/[n]? ') 
                            if confirm:
                                del log[i]
                                log_path = self.get_path(machine, date, simulation) + '/log.json'
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

    def finalize(self, sim):
        simdir = self.get_data_dir() + f'/{sim.date}/{sim.name}'
        existing = os.listdir(simdir)
        sim_id = 0
        while True:
            if str(sim_id) not in existing:
                break
            self.id += 1
        self.yell(f'[This simulation has been {sim.group}:{sim.name}:{sim_id}')
        simiddir = simdir + f'/{sim_id}'

        if not os.path.exists(simiddir):
            self.yell(f'{simiddir} does not exist. Directory created')
            os.makedirs(simiddir)
        else:
            self.yell('WARNING')

        self.write_log(sim, sim_id)

        

    def get_path(self, machine, date='', simulation='', sim_id=''):

        if not machine in self.machines:
            self.yell(f'{machine} is not in the list of registered servers')
            return None

        path = ''
        if self.get_current_machine().name == machine:
            path = self.machines[machine].datadir
        else: #share
            path = self.machines[machine].common

        if date == '':
            return path
        elif simulation == '':
            return f'{path}/{date}'
        elif sim_id == '':
            return f'{path}/{date}/{simulation}'
        else:
            return f'{path}/{date}/{simulation}/{sim_id}'


        else: #sharing

    def yell(self, s):
        print(f'[Grile] {s}')
    

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
        self.data = info['data']
        self.group = info['group']
        self.log = dict()
        self.starttime = None
        self.grile = grile

    def start(self):
        tme = datetime.datetime.now()
        self.starttime = tme
        self.log['date'] = f'{tme.year}-{tme.month}-{tme.day}'
        self.log['time'] = f'{tme.hour}-{tme.minute}-{tme.second}'

    def extra_info(self, info):
        self.log['extra_info'] = info


    def end(self):
        if not self.starttime is None:
            tme = datetime.datetime.now()
            self.log['duration'] = (tme - self.starttime).seconds
        self.ended = True
        

        self.grile.finalize(self)

        
    def record_param(self, params):
        self.log['params'] = params    



