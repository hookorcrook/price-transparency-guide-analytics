from provider_rate_parser import DataProcessor
import os


def main():
    processor = DataProcessor(output_directory='outputfiles')
    out_of_network_filepath = os.path.join(os.getcwd(), 'files/out-network')
    processor.process_out_of_network_files(out_of_network_filepath)

if __name__ == "__main__":
    main()