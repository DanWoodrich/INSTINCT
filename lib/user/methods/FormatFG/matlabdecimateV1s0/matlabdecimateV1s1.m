%this script will run from command line. Accepts two arguments: full 
%path of .wav and target decimation. 

%TestDecimate 512 "//161.55.120.117/NMML_AcousticsData/Audio_Data/Waves/AW12_AU_BS02/01_2013/AU-AWBS02-130101-020000.wav,//161.55.120.117/NMML_AcousticsData/Audio_Data/Waves/AW12_AU_BS02/01_2013/AU-AWBS02-130101-021000.wav"

%TestDecimate 512 //161.55.120.117/NMML_AcousticsData/Audio_Data/Waves/AW12_AU_BS02/01_2013/AU-AWBS02-130101-020000.wav#//161.55.120.117/NMML_AcousticsData/Audio_Data/Waves/AW12_AU_BS02/01_2013/AU-AWBS02-130101-021000.wav
function mDecimate(sfRootPathRaw,sfRootPathDec,filelistPath,target_fs_str,varargin)

    target_fs = str2num(target_fs_str);
    
    %get the file list from the csv
    FID = fopen(filelistPath);
    data = textscan(FID,'%s');
    fclose(FID);
    filelist = string(data{:});
   
    for i = 1:length(filelist)
        
        filepath = filelist(i);
        filepath = char(filepath);
        slashes = strfind(filepath,"/");
        
        prefilename = char(extractBetween(filepath,1,slashes(length(slashes))-1));
        filename = char(extractBetween(filepath,slashes(length(slashes))+1,strlength(filepath)));

        dirpath =  strcat(sfRootPathDec,'/',target_fs_str,'/',prefilename);

        filepathout = strcat(dirpath,'/',filename);

        if ~isfile(filepathout)
            
            %get full wav path 
            prefilepath =  strcat(sfRootPathRaw,'/',filepath);

            info = audioinfo(prefilepath);

            rs2 = resample(audioread(prefilepath),target_fs,info.SampleRate);

            mkdir(dirpath);

            audiowrite(filepathout,rs2,target_fs);
        end
    end
end


