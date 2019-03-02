""" Unittests for the code convering the pegasus XML workflow models
into the wf_aware manifests. 

 python -m unittest test_ManiestMaker
"""

import unittest
from workflows import (_get_jobs_and_deps, _fuse_jobs,
                       _fuse_deps, _get_jobs_names, _rename_jobs,
                       _produce_resource_steps, _encode_manifest_dic,
                       _reshape_job, _fuse_sequence_jobs, 
                       _fuse_two_jobs_sequence)
import xml.etree.ElementTree as ET

class TestManifestMaker(unittest.TestCase):
     
    def test_get_jobs_and_deps(self):
        
        xml_wf=ET.parse("./floodplain.xml")
        jobs, deps = _get_jobs_and_deps(xml_wf)
        
        self.assertEqual(jobs,
                         [{"id":"sin", "name":"SWAN Inner North",
                           "runtime":14400,"cores":160},
                          {"id":"adcirc2", "name":"Adcirc", "runtime":16200.0, 
                           "cores":256},
                         {"id":"sos", "name":"SWAN Outer South", 
                          "runtime":28800, "cores":10},
                         {"id":"adcirc", "name":"Adcirc", "runtime":39600,
                          "cores":256},
                         {"id":"son", "name":"SWAN Outer North",
                          "runtime":46800, "cores":8},
                         {"id":"sis", "name":"SWAN Inner South",
                          "runtime":10800, "cores":192},
                         {"id":"ww3", "name":"WaveWatchIII", "runtime":3600,
                          "cores":256}
                          ])
        self.assertEqual(deps,
                         {"son":["sin"],
                          "adcirc":["sin", "sos", "son", "sis"],
                          "sis":["adcirc2"],
                          "sin":["adcirc2"],
                          "ww3":["sos", "son"],
                          "sos":["sis"]
                          }
                         )
    def test_fuse_jobs(self):
        jobs = [{"id":"sin", "name":"SWAN Inner North",
                           "runtime":14400,"cores":160},
                {"id":"sin2", "name":"SWAN Inner North",
                           "runtime":12400,"cores":150},
                {"id":"adcirc2", "name":"Adcirc", "runtime":16200.0, 
                   "cores":256},
                 {"id":"sos", "name":"SWAN Outer South", 
                  "runtime":28800, "cores":10},
                 {"id":"adcirc", "name":"Adcirc", "runtime":39600,
                  "cores":256},
                 {"id":"son", "name":"SWAN Outer North",
                  "runtime":46800, "cores":8},
                 {"id":"sis", "name":"SWAN Inner South",
                  "runtime":10800, "cores":192},
                 {"id":"ww3", "name":"WaveWatchIII", "runtime":3600,
                  "cores":256}
                  ]
        new_jobs, fused_jobs_dic = _fuse_jobs(jobs, ["SWAN Inner North"])
        self.assertEqual(new_jobs,
               [{"id":"SWAN Inner North", "name":"SWAN Inner North",
                           "runtime":14400,"cores":310},
                {"id":"adcirc2", "name":"Adcirc", "runtime":16200.0, 
                   "cores":256},
                 {"id":"sos", "name":"SWAN Outer South", 
                  "runtime":28800, "cores":10},
                 {"id":"adcirc", "name":"Adcirc", "runtime":39600,
                  "cores":256},
                 {"id":"son", "name":"SWAN Outer North",
                  "runtime":46800, "cores":8},
                 {"id":"sis", "name":"SWAN Inner South",
                  "runtime":10800, "cores":192},
                 {"id":"ww3", "name":"WaveWatchIII", "runtime":3600,
                  "cores":256}
                  ])
        self.assertEqual(fused_jobs_dic, {"SWAN Inner North":["sin", "sin2"]})
    
    def test_reshape_job(self):
        job = {"id":"sin", "name":"SWAN Inner North",
                           "runtime":20,"cores":100,
                           "task_count":20,
                           "acc_runtime":400}
        new_runtime, max_cores = _reshape_job(job, 10)
        
        self.assertEqual(new_runtime,200)        
        self.assertEqual(max_cores,10)
    
    def test_fuse_jobs_max_cores(self):
        jobs = [{"id":"sin", "name":"SWAN Inner North",
                           "runtime":14400,"cores":160},
                {"id":"sin2", "name":"SWAN Inner North",
                           "runtime":14400,"cores":160},
                {"id":"adcirc2", "name":"Adcirc", "runtime":16200.0, 
                   "cores":256},
                 {"id":"sos", "name":"SWAN Outer South", 
                  "runtime":28800, "cores":10},
                 {"id":"adcirc", "name":"Adcirc", "runtime":39600,
                  "cores":256},
                 {"id":"son", "name":"SWAN Outer North",
                  "runtime":46800, "cores":8},
                 {"id":"sis", "name":"SWAN Inner South",
                  "runtime":10800, "cores":192},
                 {"id":"ww3", "name":"WaveWatchIII", "runtime":3600,
                  "cores":256}
                  ]
        new_jobs, fused_jobs_dic = _fuse_jobs(jobs, ["SWAN Inner North"],
                                              max_cores=160)
        self.assertEqual(new_jobs,
               [{"id":"SWAN Inner North", "name":"SWAN Inner North",
                           "runtime":28800,"cores":160},
                {"id":"adcirc2", "name":"Adcirc", "runtime":16200.0, 
                   "cores":256},
                 {"id":"sos", "name":"SWAN Outer South", 
                  "runtime":28800, "cores":10},
                 {"id":"adcirc", "name":"Adcirc", "runtime":39600,
                  "cores":256},
                 {"id":"son", "name":"SWAN Outer North",
                  "runtime":46800, "cores":8},
                 {"id":"sis", "name":"SWAN Inner South",
                  "runtime":10800, "cores":192},
                 {"id":"ww3", "name":"WaveWatchIII", "runtime":3600,
                  "cores":256}
                  ])
        self.assertEqual(fused_jobs_dic, {"SWAN Inner North":["sin", "sin2"]})
    
        
        
        
    def test_fuse_deps(self):
        orig_deps = {"son":["sin", "sin2"],
                    "adcirc":["sin", "sin2", "sos", "son", "sis"],
                    "sis":["adcirc2"],
                    "sin":["adcirc2"],
                    "sin2":["adcirc2"],
                    "ww3":["sos", "son"],
                    "sos":["sis"]}
        fused_jobs_dic = {"SWAN Inner North":["sin", "sin2"]}

        fused_deps=_fuse_deps(orig_deps, fused_jobs_dic)
        for (key, deps) in fused_deps.items():
            fused_deps[key]=sorted(deps)
        self.assertEqual((fused_deps),
                        ({"son":["SWAN Inner North"],
                        "adcirc":sorted(["SWAN Inner North","sos", "son",
                                         "sis"]),
                        "sis":["adcirc2"],
                        "SWAN Inner North":["adcirc2"],
                        "ww3":sorted(["sos", "son"]),
                        "sos":["sis"]}))
    
    def test_fuse_two_jobs_sequence(self):
        jobs = [{"id":"job1", "name":"acction1",
                        "runtime":100,"cores":10},
                {"id":"job2", "name":"acction2",
                        "runtime":125,"cores":10},
                {"id":"job3", "name":"acction3",
                        "runtime":125,"cores":15},
                {"id":"job4", "name":"acction4",
                        "runtime":100,"cores":10},
                {"id":"job5", "name":"acction5",
                        "runtime":125,"cores":10}]
        deps = {"job1":["job2"],
                "job2":["job3"],
                "job3":["job4"],
                "job4":["job5"]}
        _fuse_two_jobs_sequence(jobs, deps, "job1", "job2")
        self.assertEqual(jobs,
                         [{"id":"job1", "name":"acction1",
                        "runtime":225,"cores":10},
                          {"id":"job3", "name":"acction3",
                        "runtime":125,"cores":15},
                          {"id":"job4", "name":"acction4",
                        "runtime":100,"cores":10},
                          {"id":"job5", "name":"acction5",
                        "runtime":125,"cores":10}]
                         )
        self.assertEqual(deps,
                         {"job1":["job3"],
                          "job3":["job4"],
                          "job4":["job5"]})
        
        _fuse_two_jobs_sequence(jobs, deps, "job4", "job5")
        self.assertEqual(jobs,
                         [{"id":"job1", "name":"acction1",
                        "runtime":225,"cores":10},
                          {"id":"job3", "name":"acction3",
                        "runtime":125,"cores":15},
                          {"id":"job4", "name":"acction4",
                        "runtime":225,"cores":10}]
                         )
        self.assertEqual(deps,
                         {"job1":["job3"],
                          "job3":["job4"]})
    
    def test_fuse_sequence_jobs(self):
        jobs = [{"id":"job1", "name":"acction1",
                        "runtime":100,"cores":10},
                {"id":"job2", "name":"acction2",
                        "runtime":125,"cores":10},
                {"id":"job3", "name":"acction3",
                        "runtime":125,"cores":15},
                {"id":"job4", "name":"acction4",
                        "runtime":100,"cores":10},
                {"id":"job5", "name":"acction5",
                        "runtime":125,"cores":10}]
        deps = {"job1":["job2"],
                "job2":["job3"],
                "job3":["job4"],
                "job4":["job5"]}
        _fuse_sequence_jobs(jobs, deps)
        self.assertEqual(jobs,
                         [{"id":"job1", "name":"acction1",
                        "runtime":225,"cores":10},
                          {"id":"job3", "name":"acction3",
                        "runtime":125,"cores":15},
                          {"id":"job4", "name":"acction4",
                        "runtime":225,"cores":10}]
                         )
        self.assertEqual(deps,
                         {"job1":["job3"],
                          "job3":["job4"]})
        
    
    
    def test_get_job_names(self):
        
        jobs = [{"id":"sin", "name":"SWAN Inner North",
                           "runtime":14400,"cores":160},
                          {"id":"adcirc2", "name":"Adcirc", "runtime":16200.0, 
                           "cores":256},
                         {"id":"sos", "name":"SWAN Outer South", 
                          "runtime":28800, "cores":10},
                         {"id":"adcirc", "name":"Adcirc", "runtime":39600,
                          "cores":256},
                         {"id":"son", "name":"SWAN Outer North",
                          "runtime":46800, "cores":8},
                         {"id":"sis", "name":"SWAN Inner South",
                          "runtime":10800, "cores":192},
                         {"id":"ww3", "name":"WaveWatchIII", "runtime":3600,
                          "cores":256}
                          ]
        deps = {"son":["sin"],
                          "adcirc":["sin", "sos", "son", "sis"],
                          "sis":["adcirc2"],
                          "sin":["adcirc2"],
                          "ww3":["sos", "son"],
                          "sos":["sis"]
                          }
        job_names = _get_jobs_names(jobs, deps)
        print ("(JN", job_names)
        self.assertEqual(job_names, 
                         {"adcirc": "S0",
                          "ww3": "S1",
                          "sin": "S2",
                          "sis": "S3",
                          "son": "S4",
                          "sos": "S5",
                          "adcirc2": "S6",
                          })     
    def test_rename_jobs(self):
        jobs = [{"id":"sin", "name":"SWAN Inner North",
                           "runtime":14400,"cores":160},
                          {"id":"adcirc2", "name":"Adcirc", "runtime":16200.0, 
                           "cores":256},
                         {"id":"sos", "name":"SWAN Outer South", 
                          "runtime":28800, "cores":10},
                         {"id":"adcirc", "name":"Adcirc", "runtime":39600,
                          "cores":256},
                         {"id":"son", "name":"SWAN Outer North",
                          "runtime":46800, "cores":8},
                         {"id":"sis", "name":"SWAN Inner South",
                          "runtime":10800, "cores":192},
                         {"id":"ww3", "name":"WaveWatchIII", "runtime":3600,
                          "cores":256}
                          ]
        deps = {"son":["sin"],
                          "adcirc":["sin", "sos", "son", "sis"],
                          "sis":["adcirc2"],
                          "sin":["adcirc2"],
                          "ww3":["sos", "son"],
                          "sos":["sis"]
                          }
        new_job_names={"sin": "S2",
                          "adcirc2": "S6",
                          "sos": "S3",
                          "adcirc": "S0",
                          "son": "S4",
                          "sis": "S5",
                          "ww3": "S1"
                          }
        new_jobs, new_deps = _rename_jobs(jobs, deps, new_job_names)
        self.assertEqual(jobs,
                         [{"id":"S2", "name":"SWAN Inner North",
                           "runtime":14400,"cores":160},
                          {"id":"S6", "name":"Adcirc", "runtime":16200.0, 
                           "cores":256},
                         {"id":"S3", "name":"SWAN Outer South", 
                          "runtime":28800, "cores":10},
                         {"id":"S0", "name":"Adcirc", "runtime":39600,
                          "cores":256},
                         {"id":"S4", "name":"SWAN Outer North",
                          "runtime":46800, "cores":8},
                         {"id":"S5", "name":"SWAN Inner South",
                          "runtime":10800, "cores":192},
                         {"id":"S1", "name":"WaveWatchIII", "runtime":3600,
                          "cores":256}
                          ])
        self.assertEqual(new_deps,
                         {"S4":["S2"],
                          "S0":["S2", "S3", "S4", "S5"],
                          "S5":["S6"],
                          "S2":["S6"],
                          "S1":["S3", "S4"],
                          "S3":["S5"]
                          }
                         )
    def test_produce_resource_steps(self):
        jobs = [{"id":"sin", "name":"SWAN Inner North",
                           "runtime":14400,"cores":160},
                          {"id":"adcirc2", "name":"Adcirc", "runtime":16200.0, 
                           "cores":256},
                         {"id":"sos", "name":"SWAN Outer South", 
                          "runtime":28800, "cores":10},
                         {"id":"adcirc", "name":"Adcirc", "runtime":39600,
                          "cores":256},
                         {"id":"son", "name":"SWAN Outer North",
                          "runtime":46800, "cores":8},
                         {"id":"sis", "name":"SWAN Inner South",
                          "runtime":10800, "cores":192},
                         {"id":"ww3", "name":"WaveWatchIII", "runtime":3600,
                          "cores":256}
                          ]
        deps = {"son":["sin"],
                          "adcirc":["sin", "sos", "son", "sis"],
                          "sis":["adcirc2"],
                          "sin":["adcirc2"],
                          "ww3":["sos", "son"],
                          "sos":["sis"]
                          }
        resource_steps = _produce_resource_steps(jobs, deps)
        
        self.assertEqual(resource_steps,
                         [{"num_cores": 512,
                           "end_time":   3600},
                          {"num_cores": 256,
                           "end_time":   39600},
                          {"num_cores": 18,
                           "end_time":   39600+28800},
                          {"num_cores": 200,
                           "end_time":   39600+28800+10800},
                          {"num_cores": 8,
                           "end_time":   39600+46800},
                          {"num_cores": 160,
                           "end_time":   39600+46800+14400},
                          {"num_cores": 256,
                           "end_time":   39600+46800+14400+16200}
                          ]
                         )
        
    def test_enconde_manifest_dic(self):
        jobs = [{"id":"S2", "name":"SWAN Inner North",
                           "runtime":14400,"cores":160},
                          {"id":"S6", "name":"Adcirc", "runtime":16200.0, 
                           "cores":256},
                         {"id":"S3", "name":"SWAN Outer South", 
                          "runtime":28800, "cores":10},
                         {"id":"S0", "name":"Adcirc", "runtime":39600,
                          "cores":256},
                         {"id":"S4", "name":"SWAN Outer North",
                          "runtime":46800, "cores":8},
                         {"id":"S5", "name":"SWAN Inner South",
                          "runtime":10800, "cores":192},
                         {"id":"S1", "name":"WaveWatchIII", "runtime":3600,
                          "cores":256}
                          ]
        deps = {"S4":["S2"],
                  "S0":["S2", "S3", "S4", "S5"],
                  "S5":["S6"],
                  "S2":["S6"],
                  "S1":["S3", "S4"],
                  "S3":["S5"]
                  }
        manifest_dic=_encode_manifest_dic(jobs, deps)
        
        manifest_dic["tasks"] = sorted(manifest_dic["tasks"],
                                       key=lambda d: d["id"])
        self.maxDiff=None
        self.assertEqual(manifest_dic,
                         {"tasks": sorted([{"id":"S2", "name":"S2",
                           "runtime_limit":14460,
                           "runtime_sim":14400,
                           "number_of_cores":160,
                           "execution_cmd":"./S2.py"},
                          {"id":"S6", "name":"S6", 
                           "runtime_limit":16260.0,
                           "runtime_sim":16200.0, 
                           "number_of_cores":256,
                           "execution_cmd":"./S6.py"},
                         {"id":"S3", "name":"S3", 
                          "runtime_limit":28860,
                          "runtime_sim":28800,
                          "number_of_cores":10,
                           "execution_cmd":"./S3.py"},
                         {"id":"S0", "name":"S0",
                          "runtime_limit":39660,
                          "runtime_sim":39600,
                          "number_of_cores":256,
                           "execution_cmd":"./S0.py"},
                         {"id":"S4", "name":"S4",
                          "runtime_limit":46860, 
                          "runtime_sim":46800, "number_of_cores":8,
                           "execution_cmd":"./S4.py"},
                         {"id":"S5", "name":"S5",
                          "runtime_limit":10860, 
                          "runtime_sim":10800, "number_of_cores":192,
                           "execution_cmd":"./S5.py"},
                         {"id":"S1", "name":"S1",
                          "runtime_limit":3660,
                          "runtime_sim":3600,
                          "number_of_cores":256,
                           "execution_cmd":"./S1.py"}],
                                       key=lambda d: d["id"]),
                         "resource_steps": [{"num_cores": 512,
                           "end_time":   3600},
                          {"num_cores": 256,
                           "end_time":   39600},
                          {"num_cores": 18,
                           "end_time":   39600+28800},
                          {"num_cores": 200,
                           "end_time":   39600+28800+10800},
                          {"num_cores": 8,
                           "end_time":   39600+46800},
                          {"num_cores": 160,
                           "end_time":   39600+46800+14400},
                          {"num_cores": 256,
                           "end_time":   float(39600+46800+14400+16200)}
                          ],
                          "max_cores":512,
                          "total_runtime": float(39600+46800+14400+16200),
                          "dot_dag":'strict digraph "" {\n\tS2 -> S6;\n\tS3 -> S5;\n\tS0 -> S2;\n\tS0 -> S3;\n\tS0 -> S4;\n\tS0 -> S5;\n\tS4 -> S2;\n\tS5 -> S6;\n\tS1 -> S3;\n\tS1 -> S4;\n}\n'
                          })
        