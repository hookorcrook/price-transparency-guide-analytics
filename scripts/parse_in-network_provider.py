from provider_rate_parser import DataProcessor
import os


def main():
    processor = DataProcessor(output_directory='outputfiles')
    in_network_filepath = os.path.join(os.getcwd(), 'files/in-network')
    processor.process_in_network_files(in_network_filepath,generate_combined_file=True)

if __name__ == "__main__":
    main()