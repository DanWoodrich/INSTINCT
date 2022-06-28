#define basic common pipeline component shapes (used to run processes as pipelines)
from classes import INSTINCT_pipeline

class NoUpstream(INSTINCT_pipeline):

    def run(self):
        return self.run_component(final_process=True,upstream=[None])

class OneUpstream(INSTINCT_pipeline):

    def run(self):
        comp0 = self.run_component(self.compdef[0])
        comp1 = self.run_component(final_process=True,upstream=[comp0])

        return comp1

class TwoUpstream(INSTINCT_pipeline):

    def run(self):
        comp0 = self.run_component(self.compdef[0])
        comp1 = self.run_component(self.compdef[1],upstream = [comp0])
        comp2 = self.run_component(final_process=True,upstream=[comp1,comp0])
        
        return comp2

class TwoUpstream_noCon(INSTINCT_pipeline):

    def run(self):
        #if 'EDperfEval' in self.pipe_args and self.n==1:
        #    import code
        #    code.interact(local=dict(globals(), **locals()))
        comp0 = self.run_component(self.compdef[0])
        comp1 = self.run_component(self.compdef[1])

        comp2 = self.run_component(final_process=True,upstream=[comp1,comp0])         
        return comp2

class ThreeUpstream(INSTINCT_pipeline):

    def run(self):
        
        comp0 = self.run_component(self.compdef[0])
        comp1 = self.run_component(self.compdef[1],upstream = [comp0])
        comp2 = self.run_component(self.compdef[2])
        comp3 = self.run_component(final_process=True,upstream = [comp2,comp1,comp0]) 
        return comp3

class ThreeUpstream_noCon(INSTINCT_pipeline):

    def run(self):
        
        comp0 = self.run_component(self.compdef[0])
        comp1 = self.run_component(self.compdef[1])
        comp2 = self.run_component(self.compdef[2])
        comp3 = self.run_component(final_process=True,upstream = [comp2,comp1,comp0]) 
        return comp3


class ThreeUpstream_bothUpTo1(INSTINCT_pipeline):

    def run(self):
        
        comp0 = self.run_component(self.compdef[0])
        comp1 = self.run_component(self.compdef[1],upstream = [comp0])
        comp2 = self.run_component(self.compdef[2],upstream = [comp0])
        comp3 = self.run_component(final_process=True,upstream = [comp2,comp1,comp0])

        return comp3

class FourUpstream_noCon(INSTINCT_pipeline):

    def run(self):
        
        comp0 = self.run_component(self.compdef[0])
        comp1 = self.run_component(self.compdef[1])
        comp2 = self.run_component(self.compdef[2])
        comp3 = self.run_component(self.compdef[3])
        comp4 = self.run_component(final_process=True,upstream = [comp3,comp2,comp1,comp0]) 
        return comp4

class FiveUpstream_noCon(INSTINCT_pipeline):

    def run(self):
        
        comp0 = self.run_component(self.compdef[0])
        comp1 = self.run_component(self.compdef[1])
        comp2 = self.run_component(self.compdef[2])
        comp3 = self.run_component(self.compdef[3])
        comp4 = self.run_component(self.compdef[4])
        comp5 = self.run_component(final_process=True,upstream = [comp4,comp3,comp2,comp1,comp0]) 
        return comp5

#can also define pipelines for use w/o process .pipe notation
