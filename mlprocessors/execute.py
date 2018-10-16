import types
import types
import hashlib
import json
import os
import shutil
import tempfile
def sha1(str):
    hash_object = hashlib.sha1(str.encode('utf-8'))
    return hash_object.hexdigest()
def do_compute_sha1_of_file(fname):
    BLOCKSIZE = 65536
    hasher = hashlib.sha1()
    with open(fname, 'rb') as f:
        buf = f.read(BLOCKSIZE)
        while len(buf) > 0:
            hasher.update(buf)
            buf = f.read(BLOCKSIZE)
    return hasher.hexdigest()

def compute_sha1_of_file(fname,use_cache=True):
    if not os.path.exists(fname):
        return None
    if not use_cache:
        return do_compute_sha1_of_file(fname)
    stat0=os.stat(fname)
    obj=dict(
        operation='sha1',
        path=fname,
        size=stat0.st_size,
        ino=stat0.st_ino,
        mtime=stat0.st_mtime,
        ctime=stat0.st_ctime
    )
    signature_string=json.dumps(obj, sort_keys=True)
    val=pairio.getLocal(signature_string)
    if (val is not None) and (type(val)==str):
        return val
    else:
        val=compute_sha1_of_file(fname,use_cache=False)
        pairio.setLocal(signature_string,val)
        pairio.setLocal(val,fname)
        return val
    
def compute_job_input_signature(val,input_name):
    if type(val)==str:
        if val.startswith('sha1://'):
            list=str.split(val,'/')
            return list[2]
        elif os.path.exists(val):
            return compute_sha1_of_file(val)
        else:
            raise Exception('Input file does not exist: {}'.format(input_name))
    else:
        if hasattr(val,'signature'):
            return getattr(val,'signature')
        else:
            raise Exception("Unable to compute signature for input: {}".format(input_name))
            
def get_file_extension(fname):
    if type(fname)==str:
        name, ext = os.path.splitext(fname)
        return ext
    else:
        return ''
    
def compute_processor_job_output_signature(self,output_name):
    processor_inputs=[]
    job_inputs=[]
    for input0 in self.INPUTS:
        name0=input0.name
        val0=getattr(self,name0)
        processor_inputs.append(dict(
            name=name0
        ))
        job_inputs.append(dict(
            name=name0,
            signature=compute_job_input_signature(val0,input_name=name0),
            ext=get_file_extension(val0)
        ))
    processor_outputs=[]
    job_outputs=[]
    for output0 in self.OUTPUTS:
        name0=output0.name
        processor_outputs.append(dict(
            name=name0
        ))
        val0=getattr(self,name0)
        if type(val0)==str:
            job_outputs.append(dict(
                name=name0,
                ext=get_file_extension(val0)
            ))
        else:
            job_outputs.append(dict(
                name=name0,
                ext=val0['ext']
            ))
    processor_parameters=[]
    job_parameters=[]
    for param0 in self.PARAMETERS:
        name0=param0.name
        processor_parameters.append(dict(
            name=name0
        ))
        job_parameters.append(dict(
            name=name0,
            value=getattr(self,name0)
        ))
    processor_obj=dict(
        processor_name=self.NAME,
        processor_version=self.VERSION,
        inputs=processor_inputs,
        outputs=processor_outputs,
        parameters=processor_parameters
    )
    signature_obj=dict(
        processor=processor_obj,
        inputs=job_inputs,
        outputs=job_outputs,
        parameters=job_parameters
    )
    if output_name:
        signature_obj["output_name"]=output_name
    signature_string=json.dumps(signature_obj, sort_keys=True)
    return sha1(signature_string)

def create_temporary_file(fname):
    tmp=tempfile.gettempdir()+'/mlprocessors'
    if not os.path.exists(tmp):
        os.mkdir(tmp)
    return tmp+'/'+fname

class ProcessorExecuteOutput():
    def __init__(self):
        self.outputs=dict()

def execute(self, _cache=True, **kwargs):
    X=self()
    ret=ProcessorExecuteOutput()
    for input0 in self.INPUTS:
        name0=input0.name
        if not name0 in kwargs:
            raise Exception('Missing input: {}'.format(name0))
        setattr(X,name0,kwargs[name0])
    for output0 in self.OUTPUTS:
        name0=output0.name
        if not name0 in kwargs:
            raise Exception('Missing output: {}'.format(name0))
        setattr(X,name0,kwargs[name0])
    for param0 in self.PARAMETERS:
        name0=param0.name
        if not name0 in kwargs:
            raise Exception('Missing parameter: {}'.format(name0))
        setattr(X,name0,kwargs[name0])
    if _cache:
        outputs_all_in_pairio=True
        output_signatures=dict()
        output_sha1s=dict()
        for output0 in self.OUTPUTS:
            name0=output0.name
            signature0=compute_processor_job_output_signature(X,name0)
            output_signatures[name0]=signature0
            output_sha1=pairio.getLocal(signature0)
            if output_sha1 is not None:
                output_sha1s[name0]=output_sha1
            else:
                outputs_all_in_pairio=False
        output_files_all_found=False
        output_files=dict()
        if outputs_all_in_pairio:
            output_files_all_found=True
            for output0 in self.OUTPUTS:
                name0=output0.name
                sha1=output_sha1s[name0]
                hint_fname=pairio.getLocal(sha1)
                if not hint_fname:
                    hint_fname=getattr(X,name0)
                sha1b=compute_sha1_of_file(hint_fname)
                if sha1==sha1b:
                    output_files[name0]=hint_fname
                else:
                    output_files_all_found=False
        if outputs_all_in_pairio and (not output_files_all_found):
            print('Found job in cache, but not all output files exist.')
            
        if output_files_all_found:
            print('Using outputs from cache.')
            for output0 in self.OUTPUTS:
                name0=output0.name
                fname1=output_files[name0]
                fname2=getattr(X,name0)
                if type(fname2)==str:
                    if fname1!=fname2:
                        if os.path.exists(fname2):
                            os.remove(fname2)
                        shutil.copyfile(fname1,fname2)
                    ret.outputs[name0]=fname2
                else:
                    ret.outputs[name0]=fname1
            return ret
        
    for output0 in self.OUTPUTS:
        name0=output0.name
        val0=getattr(X,name0)
        job_signature0=compute_processor_job_output_signature(X,None)
        if type(val0)!=str:
            fname0=job_signature0+'_'+name0+val0['ext']
            tmp_fname=create_temporary_file(fname0)
            setattr(X,name0,tmp_fname)
    X.run()
    for output0 in self.OUTPUTS:
        name0=output0.name
        ret.outputs[name0]=getattr(X,name0)
        if _cache:
            signature0=output_signatures[name0]
            output_sha1=compute_sha1_of_file(getattr(X,name0))
            pairio.setLocal(signature0,output_sha1)
    return ret