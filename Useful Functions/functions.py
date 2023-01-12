import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from sklearn.metrics import confusion_matrix
import itertools

def plot_confusion_matrix(y_test, 
                          y_preds, 
                          figsize,
                          axis_fontsize=20,
                          title_fontsize=20,
                          annotation_fontsize=15, 
                          classes = False):
    """Plots confusion matrix given actuals and predicted labels
    args:
    y_test: array of actual labels
    y_preds: array of predicted labels
    figsize: tuple of x-y sizing dimensions
    classes: list of class names
    """
    cm = confusion_matrix(y_test, y_preds)
    cm_norm = cm.astype('float')/cm.sum(axis=1)
    n_classes = cm.shape[0]
    
    if classes:
        labels = classes
    else:
        labels = np.arange(n_classes)

    #Threshold to change color
    threshold = 0.5*(cm.max()+cm.min())

    #Plot confusion matrix
    fig, ax = plt.subplots(figsize=figsize)
    cax = ax.matshow(cm, cmap = plt.cm.Blues)
    fig.colorbar(cax)

    ax.set(title = 'Confusion matrix',
            xlabel = 'Predicted label',
            ylabel = 'True label',
            xticks = np.arange(n_classes),
            yticks = np.arange(n_classes),
            xticklabels = labels,
            yticklabels = labels
            )
    
    ax.xaxis.set_label_position('bottom')
    ax.xaxis.tick_bottom()
    ax.yaxis.label.set_size(axis_fontsize)
    ax.xaxis.label.set_size(axis_fontsize)
    ax.title.set_size(title_fontsize)

    #Plot text on each cell
    for i, j in itertools.product(range(cm.shape[0]), range(cm.shape[1])):
        plt.text(j, i, f'{cm[i,j]} ({cm_norm[i,j]*100:.1f})%',
                horizontalalignment='center',
                color = 'white' if cm[i,j]>threshold else 'black',
                size = annotation_fontsize
                )

def plot_decision_boundary(model, X, y):
    """
    Takes in a trained model with features (X) and labels (y) and plot predictions as well as line between
    classes. 
    a. Creates a meshgrid of different X values
    b. Make predictions across the meshgrid
    
    args:
    model: Trained model object - can handle neural network models
    X: list of X values. Recommended to use X_test
    y: list of true labels. Recommended to use y_test
    """
    
    #Create meshgrid
    x_min, x_max = X[:,0].min()-0.1, X[:,0].max()+0.1
    y_min, y_max = X[:,1].min()-0.1, X[:,1].max()+0.1
    xx, yy = np.meshgrid(np.linspace(x_min, x_max, 100),
                        np.linspace(y_min, y_max, 100))
    
    #Stack 2D arrays together
    x_in = np.c_[xx.ravel(), yy.ravel()]
    y_pred = model.predict(x_in)

    #Check for multi-class
    if len(y_pred[0])>1:
        print('Doing multiclass classification. Reshape predictions for plotting')
        y_pred=np.argmax(y_pred,axis=1).reshape(xx.shape)
    else:
        print("Doing binary classification")
        y_pred=np.round(y_pred).reshape(xx.shape)

    #Plot decision boundary
    plt.contourf(xx, yy, y_pred, cmap=plt.cm.RdYlBu, alpha=0.7)
    plt.scatter(X[:,0], X[:,1], c=y, s=40, cmap=plt.cm.RdYlBu)
    plt.xlim(xx.min(),xx.max())
    plt.ylim(yy.min(),yy.max())

def quick_hist(data):
    """Plots histogram on single array of data
    args:
    data: Single column of data array
    
    Source:
    https://stackoverflow.com/questions/6352740/matplotlib-label-each-bin
    """

    fig,ax = plt.subplots(figsize=(30,10))
    counts, bins, patches = ax.hist(data, bins = 30,facecolor='yellow', edgecolor='gray')

    # Set the ticks to be at the edges of the bins.
    ax.set_xticks(bins)
    # Set the xaxis's tick labels to be formatted with 1 decimal place...
    ax.xaxis.set_major_formatter(ticker.FormatStrFormatter('%0.1f'))

    # Change the colors of bars at the edges...
    twentyfifth, seventyfifth = np.percentile(data, [25, 75])
    for patch, rightside, leftside in zip(patches, bins[1:], bins[:-1]):
        if rightside < twentyfifth:
            patch.set_facecolor('green')
        elif leftside > seventyfifth:
            patch.set_facecolor('red')

    # Label the raw counts and the percentages below the x-axis...
    bin_centers = 0.5 * np.diff(bins) + bins[:-1]
    for count, x in zip(counts, bin_centers):
        # Label the raw counts
        ax.annotate(str(count), xy=(x, 0), xycoords=('data', 'axes fraction'),
            xytext=(0, -18), textcoords='offset points', va='top', ha='center')

        # Label the percentages
        percent = '%0.0f%%' % (100 * float(count) / counts.sum())
        ax.annotate(percent, xy=(x, 0), xycoords=('data', 'axes fraction'),
            xytext=(0, -32), textcoords='offset points', va='top', ha='center')

    # Give ourselves some more room at the bottom of the plot
    plt.rcParams.update({'font.size': 10})
    plt.subplots_adjust(bottom=0.3)
    plt.show()