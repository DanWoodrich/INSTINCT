jobs = {
"MPE":["PE2all","PE2_FG","ED_AM_perfEval"],
"TT":["PE2_FG_novel","PE2all","PerfEval_MPE_TT","PE2_FG","PE2all_novel"],
"NewJob":["ViewGT","ViewED"]
}

pipelines = {
    #testing!
'ViewGTxxxc':{'pipe':"pViewDETx",'pViewDETx':{'process':"RavenViewDETx",'GetDETx':{'pipe':"FormatGT.pipe",'loop_on':"FormatFG",'FormatGT.pipe':{'process':"FormatGT",'GetFG':"FormatFG"}},\
             "GetFG":{'pipe':"FormatFG.pipe",'loop_on':"FormatFG",'FormatFG.pipe':{'process':"FormatFG"}}}},
#'ViewGT':{'pipe':"pViewDETx",'loop_on':"FormatFG",'pViewDETx':{'process':"RavenViewDETx",'GetDETx':{'process':"FormatGT"},\
#             "GetFG":{'process':"FormatFG"}}}, #doesn't work, since loop is assessed on components not the product
'ViewED':{"pViewDETx":{'pipe':"RavenViewDETx","GetDETx":{'pipe':"EventDetector"},\
             "GetFG":{'pipe':"FormatFG"}}}
}
