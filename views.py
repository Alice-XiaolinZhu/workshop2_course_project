from django.shortcuts import render
from django.http import HttpResponse
from .forms import SearchForm

# Create your views here.
def index(request):
    return HttpResponse("Hello world!")

def index1(request):
    context = dict()
    if request.method == "POST":
        form = SearchForm(request.POST)
        if form.is_valid():
            post_data = form.cleaned_data['search']
        context['content'] = post_data
        return render(request, 'search.html', context)

    else:
        context['content'] = "Please input your word:"
        form = SearchForm()
    context['form'] = form
    return render(request, 'index.html', context)

def index2(request):
    context = dict()
    if request.method == "POST":
        form = SearchForm(request.POST)
        if form.is_valid():
            post_data = form.cleaned_data['search']
        context['content'] = post_data
        return render(request, 'search.html', context)

    else:
        context['content'] = "Please input your word:"
        form = SearchForm()
    context['form'] = form
    return render(request, 'index.html', context)

def search(request):
    import time
    q = request.GET.get('q')

    # initialize all the variables
    context = dict()
    words = []
    details = []
    details2 = []
    details3 = []
    file_names = []
    line_indexs = []
    tf_idf_values = []
 
    # record the start time
    start_time = time.time()
    if request.method == "POST":
        form = SearchForm(request.POST)
        if form.is_valid():
            post_data = form.cleaned_data['search']
            print(post_data)
            
            # open the file
            file = open('/home/uic/ws2_django_demo/static/data.txt')
            lines = file.readlines()
            
            for l in lines:
                word,detail = l.split(':::::')
                file_name,detail2 = detail.split('\t$$$$$[')
                line_index,detail3 = detail2.split(',@@@@@]\t')
                tf_idf,n = detail3.split('\n')
                
                if word == post_data.upper():
                    words.extend([word])
                    file_names.extend([file_name])
                    line_indexs.extend([line_index])
                    tf_idf_values.extend([tf_idf])
                    context['file_name'] = file_name
                    context['line_index'] = line_index
                
            d = dict(zip(file_names, line_indexs))
            file.close
        
        # record end time
        end_time = time.time()
        total_time = str(end_time-start_time)
        context['time'] = total_time
        context['content'] = post_data
        print('Server time: %s s' % (end_time-start_time))
        return render(request, 'search.html', {'content':post_data, 'dict':d, 'form': form, 'time':total_time})
        
    else:
        context['content'] = "Results cannot be found."
        form = SearchForm()
    
    context['form'] = form
    return render(request, 'search.html', context)

def details(request):
    import os

    q = request.GET.get('q')
    context = dict()
    articles = []

    # redefine the path and open local file
    file_path = os.path.join('/home/uic/ws2_django_demo/static/project_data/'+ q)
    file = open(file_path, 'r')
    lines = file.readlines()
    for l in lines:
        l = l.strip()
        articles.extend([l])
    file.close
    context['title'] = q
    context['article'] = articles
    return render(request, 'details.html', {'title':q, 'article':articles})
    
    

