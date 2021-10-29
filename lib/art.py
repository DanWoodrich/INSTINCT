import os

def getArt(project,result,num=1):
    from user.art import getArt as u_art
    art = u_art(project,result,num=1)
    #template for art by artist 'snd' https://ascii.co.uk/art/whale
    #modified by dfw!
    if art ==None:
        if result==True and num < 10: 
            art =r"""                                ','. '. ; : ,','
                                   '..'.,',..'
                                    ';.'  ,'
                                       ;;
                                       ;'
                      :._   _.------------.__
              __      |  :-'              ## '\
       __   ,' .'    .'             /        ##\ 
     /__ '.-   \___.'                ^  .--.  # |
       '._                  ~~      \__/    \__/
         '----'.____           \      ##     .'
                    '------.    \._____.----' 
             INSTINCT       \.__/  
            """
        elif result==True and num==10:
            art =r"""                                ','. '. ; : ,','
                                   '..'.,',..'
                                    ';.'  ,'
                                        ;;
                                       _____
                                      /     \___
                      :._   _.-------+=======+--`
              __      |  :-'              ## '\       )
       __   ,' .'    .'            __~       ##\     (
     /__ '.-   \___.'                D  .--.  # |     )
       '._                  ~~      \__/    \__[======#
         '----'.____           \      ##     .'
                    '------.    \._____.----' 
             INSTINCT       \.__/  
            """
        else:    
            art =r"""                                ','. '. ; : ,','
                                   '..'.,',..'
                                    ';.'  ,'
                                       ;;
                                       ;'
                      :._   _.------------.__
              __      |  :-'              ## '\
       __   ,' .'    .'            /         ##\ 
     /__ '.-   \___.'              o  .----.  # |
       '._                  ~~       /   ## \__/
         '----'.____           \    / ##     .'
                    '------.    \._____.----' 
             INSTINCT       \.__/  
            """
    return art

    print("Signal art not yet defined")
