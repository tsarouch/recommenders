import matplotlib.pyplot as plt

def plot_roc_curves(datapoints_list,
                  labels_list,
                  colors_list):
    """ Plot the ROC curves

    :datapoints_list: e.g. [[[1, 1], [2, 4], [3, 9], [4, 16]], ]
    :labels_list: a list of strings corresponding to a label for each curve
    e.g. "ALS model"
    :colors_list: a list of colors to easily separate the different models
    """

    plt.title('ROC curve')
    for i in range(len(datapoints_list)):
        x_vec = [x[0] for x in datapoints_list[i]]
        y_vec = [x[1] for x in datapoints_list[i]]
        plt.plot(x_vec, y_vec, colors_list[i] + 'o--', label=labels_list[i])
    plt.ylabel('True Positive Rate')
    plt.xlabel('False Positive Rate')
    plt.legend()
    plt.axis([0, 6, 0, 20])
    plt.show()
