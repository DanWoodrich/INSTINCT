jobs = {
"MPExxx":["PE2all","PE2_FG","ED_AM_perfEval"],
"TT_testxxxx":["ViewGT","MPE"],
"NewJobxxxx":["ViewGT","ViewED"]
}

pipelines = {
    #testing!
'ViewGTxxxxxx':{'pipe':"pViewDETx",'pViewDETx':{'process':"RavenViewDETx",'GetDETx':{'pipe':"FormatGT",'loop_on':"FormatFG",'FormatGT':{'process':"FormatGT"}},\
             "GetFG":{'pipe':"FormatFG",'loop_on':"FormatFG",'FormatFG':{'process':"FormatFG"}}}},
#'ViewGT':{'pipe':"pViewDETx",'loop_on':"FormatFG",'pViewDETx':{'process':"RavenViewDETx",'GetDETx':{'process':"FormatGT"},\
#             "GetFG":{'process':"FormatFG"}}}, #doesn't work, since loop is assessed on components not the product
'ViewEDxxxx':{"pViewDETx":{'pipe':"RavenViewDETx","GetDETx":{'pipe':"EventDetector"},\
             "GetFG":{'pipe':"FormatFG"}}}
}
