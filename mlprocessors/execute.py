import types
import types
import hashlib
import json
import os
import shutil
import tempfile
from pairio import client as pairio
from kbucket import client as kbucket

def sha1(str):
    hash_object = hashlib.sha1(str.encode('utf-8'))
    return hash_object.hexdigest()
        
def compute_job_input_signature(val,input_name):
    if type(val)==str:
        if val.startswith('sha1://'):
            list=str.split(val,'/')
            return list[2]
        elif os.path.exists(val):
            return kbucket.computeFileSha1(val) ## todo: kbucket.computeFileSha1 should include sha1:// and kbucket:// paths
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

def execute(proc, _cache=True, _force_run=False, **kwargs):
    X=proc() # instance
    ret=ProcessorExecuteOutput()
    for input0 in proc.INPUTS:
        name0=input0.name
        if not name0 in kwargs:
            raise Exception('Missing input: {}'.format(name0))
        setattr(X,name0,kwargs[name0])
    for output0 in proc.OUTPUTS:
        name0=output0.name
        if not name0 in kwargs:
            raise Exception('Missing output: {}'.format(name0))
        setattr(X,name0,kwargs[name0])
    for param0 in proc.PARAMETERS:
        name0=param0.name
        if not name0 in kwargs:
            raise Exception('Missing parameter: {}'.format(name0))
        setattr(X,name0,kwargs[name0])
    if _cache:
        outputs_all_in_pairio=True
        output_signatures=dict()
        output_sha1s=dict()
        cache_collections=set()
        for output0 in proc.OUTPUTS:
            name0=output0.name
            signature0=compute_processor_job_output_signature(X,name0)
            output_signatures[name0]=signature0
            output_sha1,output_collection=pairio.get(signature0,return_collection=True)
            if output_sha1 is not None:
                print('Found output "{}" in cache collection: {}'.format(name0,output_collection))
                cache_collections.add(output_collection)
                output_sha1s[name0]=output_sha1
            else:
                outputs_all_in_pairio=False
        output_files_all_found=False
        output_files=dict()
        if outputs_all_in_pairio:
            output_files_all_found=True
            for output0 in proc.OUTPUTS:
                name0=output0.name
                sha1=output_sha1s[name0]
                fname=kbucket.findFile(sha1=sha1)
                if fname:
                    output_files[name0]=fname
                else:
                    output_files_all_found=False
        if outputs_all_in_pairio and (not output_files_all_found):
            print('Found job in cache, but not all output files exist.')

        if output_files_all_found:
            if not _force_run:
                print('Using outputs from cache:',','.join(list(cache_collections)))
                for output0 in proc.OUTPUTS:
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
            else:
                print('Found outputs in cache, but forcing run...')
        
    temporary_output_files=set()
    for output0 in proc.OUTPUTS:
        name0=output0.name
        val0=getattr(X,name0)
        job_signature0=compute_processor_job_output_signature(X,None)
        if type(val0)!=str:
            fname0=job_signature0+'_'+name0+val0['ext']
            tmp_fname=create_temporary_file(fname0)
            temporary_output_files.add(tmp_fname)
            setattr(X,name0,tmp_fname)
    try:
        X.run()
    except:
        # clean up temporary output files
        for fname in temporary_output_files:
            if os.path.exists(fname):
                os.remove(fname)
        raise
    for output0 in proc.OUTPUTS:
        name0=output0.name
        output_fname=getattr(X,name0)
        if output_fname in temporary_output_files:
            output_fname=kbucket.moveFileToCache(output_fname)
        ret.outputs[name0]=output_fname
        if _cache:
            output_sha1=kbucket.computeFileSha1(output_fname)
            signature0=output_signatures[name0]
            print('setting: '+signature0+':'+output_sha1)
            pairio.set(signature0,output_sha1)
    return ret

