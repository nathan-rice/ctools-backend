import os
import numpy as np
import math
import matplotlib as mpl
import matplotlib

from ctools_backend import geo
import models

matplotlib.use("Agg")  # needs to be before pyplot import

from matplotlib import pyplot as plt
from matplotlib import cm
from scipy import interpolate


class RasterGenerator(object):
    def __init__(self, scenario_run):
        self.scenario_run = scenario_run
        self.pollutant = {"1": "NOX", "2": "Benz", "3": "PM2.5", "4": "D_PM2.5", "5": "EC_2.5",
                          "6": "OC_2.5", "7": "CO", "8": "FORM", "9": "ALD2", "10": "ACRO",
                          "11": "1,3-BUTA"}[scenario_run.pollutant]
        if self.pollutant in ("1", "7"):
            units = " (ppb)"
        else:
            units = "  ($\mu g/m^{3}$)"
        is_comparison_run = isinstance(scenario_run, models.ComparisonScenarioRun)
        if is_comparison_run and self.scenario_run.comparison_mode == 1:
            if self.scenario_run.model_type == 2:
                self._legend_text = self.pollutant + " annual average difference" + units
            elif self.scenario_run.model_type == 3:
                self._legend_text = self.pollutant + " cancer risk difference (incidence per million)"
            elif self.scenario_run.model_type == 4:
                self._legend_text = self.pollutant + " non-cancer risk difference (incidence per million)"
            else:
                self._legend_text = self.pollutant + " concentration difference" + units
        elif is_comparison_run and self.scenario_run.comparison_mode == "Relative (%)":
            self.legend_max = 100
            self.legend_min = - 100
            if self.scenario_run.model_type == 2:
                self._legend_text = self.pollutant + " annual average difference (%)"
            if self.scenario_run.model_type == 3:
                self._legend_text = self.pollutant + " cancer risk difference (%)"
            elif self.scenario_run.model_type == 4:
                self._legend_text = self.pollutant + " non-cancer risk difference (%)"
            else:
                self._legend_text = self.pollutant + " concentration difference (%)"
        else:
            if self.scenario_run.model_type == 2:
                self._legend_text = self.pollutant + " annual average" + units
            elif self.scenario_run.model_type == 3:
                self._legend_text = self.pollutant + " cancer risk (incidence per million)"
            elif self.scenario_run.model_type == 4:
                self._legend_text = self.pollutant + " non-cancer risk (incidence per million)"
            else:
                self._legend_text = self.pollutant + " concentration" + units

    def create_pollution_raster(self, concentrations):
        coordinates = concentrations[:, 0:2]
        lat_lng = np.array([geo.lcc_to_mercator(x, y) for (x, y) in coordinates])
        conc = concentrations[:, 2]
        # record various information about the data for later use
        max_lat = np.max(lat_lng[:, 1])
        min_lat = np.min(lat_lng[:, 1])
        max_lng = np.max(lat_lng[:, 0])
        min_lng = np.min(lat_lng[:, 0])
        lat_delta = max_lat - min_lat
        lng_delta = max_lng - min_lng

        interp_lat, interp_lng = self.build_interpolation_grids(lat_delta, lng_delta)

        lng_step = lng_delta / len(interp_lng)
        lat_step = lat_delta / len(interp_lng[0])  # Y grid should have the same dimensions
        # This should cause the values in the array to start at the minimum lat/long, and end at the maximum
        interp_lng = interp_lng * lng_step + min_lng
        interp_lat = interp_lat * lat_step + min_lat
        interp_lat_lng = self.transform_interpolation_grid_to_predictor_input(interp_lat, interp_lng)
        interp_x_y = np.array([geo.mercator_to_lcc(lng, lat) for (lat, lng) in interp_lat_lng])
        if self.scenario_run.model_min_value:
            conc[conc < self.scenario_run.model_min_value] = self.scenario_run.model_min_value
        if self.scenario_run.model_max_value:
            conc[conc > self.scenario_run.model_max_value] = self.scenario_run.model_max_value
        results = interpolate.griddata(coordinates, conc, interp_x_y, fill_value=0, method='linear')
        img_data = self.transform_array_to_image_data(results, len(interp_lng), len(interp_lng[0]))
        self.create_concentration_image(img_data)
        self.create_legend_img(concentrations)

    @staticmethod
    def transform_array_to_image_data(array, width, height):
        return np.rot90(array.reshape((width, height)))

    def create_concentration_image(self, image_data):
        cmap = cm.get_cmap()
        cmap._init()
        is_comparison_run = isinstance(self.scenario_run, models.ComparisonScenarioRun)
        if not is_comparison_run or self.scenario_run.comparison_mode == "Absolute":
            alphas = np.abs([min(n, 1.0) for n in np.linspace(0, 2, cmap.N)])
            vmax = np.max(image_data)
            vmin = np.min(image_data)
        else:
            results_max = np.max(image_data)
            results_min = np.min(image_data)
            if np.abs(results_max) > np.abs(results_min):
                vmax = results_max
                vmin = -results_max
            else:
                vmax = -results_min
                vmin = results_min
            results_range = vmax - vmin
            value_array = np.linspace(vmin, vmax, cmap.N)
            alphas = np.array([min(np.abs(v) / results_range * 2, 1.0) for v in value_array])
        cmap._lut[:-3, -1] = alphas
        if is_comparison_run:
            output_directory = self.scenario_run.output_directory_1
        else:
            output_directory = self.scenario_run.output_directory
        plt.imsave(fname=os.path.join(output_directory, "concentrations.png"),
                   arr=image_data, format='png', vmax=vmax, vmin=vmin)

    def create_legend_img(self, concentrations):
        def label(num):
            if num == 0:
                return "0"
            elif num > 0:
                return "$10^{%d}$" % num
            else:
                return "$-10^{%d}$" % abs(num)
        fig, ax = plt.subplots()
        fig.set_figheight(4)
        fig.set_figwidth(0.4)
        min_ = self.scenario_run.model_min_value
        max_ = self.scenario_run.model_max_value
        is_comparison_run = isinstance(self.scenario_run, models.ComparisonScenarioRun)
        if is_comparison_run and self.scenario_run.comparison_mode == "Relative":
            if not min_:
                min_ = np.min(concentrations[:, 2])
            if not max_:
                max_ = np.max(concentrations[:, 2])
            if np.abs(max_) > np.abs(min_):
                min_ = -max_
            else:
                max_ = -min_
            norm = mpl.colors.Normalize(vmin=min_, vmax=max_)
            ticks = np.linspace(math.ceil(min_), math.floor(max_),
                                np.abs(math.ceil(min_)) + np.abs(math.floor(max_)) + 1)
            cb = mpl.colorbar.ColorbarBase(ax, norm=norm, ticks=ticks, orientation='vertical')
            cb.ax.set_yticklabels([label(v) for v in ticks])
        elif is_comparison_run and self.scenario_run.comparison_mode == "Relative (%)":
            if min_ == self.scenario_run.model_min_value:
                min_ = max(np.min(concentrations[:, 2]), min_)
            if max_ == self.scenario_run.model_max_value:
                max_ = min(np.max(concentrations[:, 2]), max_)
            if np.abs(max_) > np.abs(min_):
                min_ = -max_
            else:
                max_ = -min_
            norm = mpl.colors.Normalize(vmin=min_, vmax=max_)
            cb = mpl.colorbar.ColorbarBase(ax, norm=norm, orientation='vertical')
        else:
            if not min_:
                min_ = (10 ** np.min(concentrations[:, 2]))
            if not max_:
                max_ = (10 ** np.max(concentrations[:, 2]))
            norm = mpl.colors.LogNorm(vmin=min_, vmax=max_)
            cb = mpl.colorbar.ColorbarBase(ax, norm=norm, orientation='vertical')
        cb.set_label(self._legend_text)
        if is_comparison_run:
            output_directory = self.scenario_run.output_directory_1
        else:
            output_directory = self.scenario_run.output_directory
        img = os.path.join(output_directory, "concentrations_legend.png")
        fig.savefig(img, dpi=100, bbox_inches='tight')

    @staticmethod
    def build_interpolation_grids(lat_delta, lng_delta, max_size=1600):
        if lat_delta < lng_delta:
            x_range = np.arange(0, int(max_size * lat_delta / lng_delta))
            y_range = np.arange(0, max_size)
            (interp_x, interp_y) = np.meshgrid(x_range, y_range)
        else:
            x_range = np.arange(0, max_size)
            y_range = np.arange(0, int(max_size * lng_delta / lat_delta))
            (interp_x, interp_y) = np.meshgrid(x_range, y_range)
        return interp_x, interp_y

    @staticmethod
    def transform_interpolation_grid_to_predictor_input(x_grid, y_grid):
        return np.array(zip(x_grid.ravel(), y_grid.ravel()))
