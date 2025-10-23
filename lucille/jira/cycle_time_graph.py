import pandas as pd
import argparse
import numpy as np
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

class SprintCycleTimeAnalyzer:
    """Analyze Jira sprint transition data to identify cycle time bottlenecks"""

    CANONICAL_STATES = [
        "Ready for Development",
        "In Progress",
        "Review",
        "Ready for Testing",
        "In Testing",
        "To Deploy",
        "Done"
    ]

    def __init__(self, csv_path):
        """Load and prepare sprint transition data"""
        self.df = pd.read_csv(csv_path)
        self.df['transition_timestamp'] = pd.to_datetime(self.df['transition_timestamp'])
        self.df = self.df.sort_values(['issue_key', 'transition_timestamp'])

    def calculate_business_hours(self, start_time, end_time):
        """
        Calculate business hours between two timestamps, excluding weekends.

        Args:
            start_time: datetime object for start
            end_time: datetime object for end

        Returns:
            float: Number of hours excluding weekends
        """
        if start_time >= end_time:
            return 0

        total_hours = 0
        current = start_time

        # Process full days
        while current.date() < end_time.date():
            # Check if current day is a weekday (Monday=0, Sunday=6)
            if current.weekday() < 5:  # Monday to Friday
                # Calculate hours remaining in current day
                end_of_day = current.replace(hour=23, minute=59, second=59, microsecond=999999)
                hours_in_day = (end_of_day - current).total_seconds() / 3600
                total_hours += hours_in_day

            # Move to start of next day
            current = (current + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

        # Add remaining hours on the final day if it's a weekday
        if current.weekday() < 5 and end_time > current:
            final_hours = (end_time - current).total_seconds() / 3600
            total_hours += final_hours

        return total_hours

    def calculate_time_in_states(self):
        """Calculate time spent in each state for every issue, excluding weekends"""
        results = []

        for issue_key, group in self.df.groupby('issue_key'):
            issue_data = {
                'issue_key': issue_key,
                'summary': group.iloc[0]['summary'],
                'issue_type': group.iloc[0]['issue_type'],
                'current_status': group.iloc[0]['current_status']
            }

            # Calculate time in each state
            transitions = group.sort_values('transition_timestamp')
            for i in range(len(transitions) - 1):
                current_state = transitions.iloc[i]['to_status']
                next_transition = transitions.iloc[i + 1]['transition_timestamp']
                current_time = transitions.iloc[i]['transition_timestamp']

                # Calculate business hours (excluding weekends)
                time_in_state = self.calculate_business_hours(current_time, next_transition)

                state_key = f'hours_in_{current_state.lower().replace(" ", "_")}'
                if state_key not in issue_data:
                    issue_data[state_key] = 0
                issue_data[state_key] += time_in_state

            results.append(issue_data)

        return pd.DataFrame(results)

    def analyze_deployment_bottleneck(self, cycle_df):
        """Focus on the 'To Deploy' bottleneck"""
        deploy_col = 'hours_in_to_deploy'

        if deploy_col not in cycle_df.columns:
            print("⚠️  No issues went through 'To Deploy' state")
            return None

        deploy_data = cycle_df[cycle_df[deploy_col].notna()].copy()
        deploy_data['days_in_to_deploy'] = deploy_data[deploy_col] / 24

        print("\n" + "="*70)
        print("🚨 DEPLOYMENT BOTTLENECK ANALYSIS (Business Days Only)")
        print("="*70)

        print(f"\n📊 Overall Statistics:")
        print(f"  • Issues that went through 'To Deploy': {len(deploy_data)}")
        print(f"  • Average time in 'To Deploy': {deploy_data['days_in_to_deploy'].mean():.1f} business days")
        print(f"  • Median time in 'To Deploy': {deploy_data['days_in_to_deploy'].median():.1f} business days")
        print(f"  • Max time in 'To Deploy': {deploy_data['days_in_to_deploy'].max():.1f} business days")

        print(f"\n🔥 Top 10 Worst Offenders:")
        worst = deploy_data.nlargest(10, 'days_in_to_deploy')
        for idx, row in worst.iterrows():
            print(f"\n  {row['issue_key']} ({row['days_in_to_deploy']:.1f} business days)")
            print(f"    {row['summary'][:65]}")
            print(f"    Status: {row['current_status']}")

        return deploy_data

    def analyze_post_dev_cycle_time(self, cycle_df):
        """Analyze complete post-development cycle (Ready for Testing -> Done)"""
        # Calculate total post-dev time
        post_dev_cols = [
            'hours_in_ready_for_testing',
            'hours_in_in_testing',
            'hours_in_to_deploy'
        ]

        # Filter to issues that have at least one post-dev state
        has_post_dev = cycle_df[[col for col in post_dev_cols if col in cycle_df.columns]].notna().any(axis=1)
        post_dev_df = cycle_df[has_post_dev].copy()

        # Fill NaN with 0 for calculation
        for col in post_dev_cols:
            if col not in post_dev_df.columns:
                post_dev_df[col] = 0
            else:
                post_dev_df[col] = post_dev_df[col].fillna(0)

        post_dev_df['total_post_dev_hours'] = post_dev_df[post_dev_cols].sum(axis=1)
        post_dev_df['total_post_dev_days'] = post_dev_df['total_post_dev_hours'] / 24

        print("\n" + "="*70)
        print("⏱️  POST-DEVELOPMENT CYCLE TIME (Ready for Testing -> Done)")
        print("    Business Days Only - Weekends Excluded")
        print("="*70)

        avg_total = post_dev_df['total_post_dev_hours'].mean()

        print(f"\n📊 Average Post-Dev Cycle Time: {avg_total / 24:.1f} business days")
        print(f"\n  Breakdown by stage:")

        for col, label in [
            ('hours_in_ready_for_testing', 'Ready for Testing'),
            ('hours_in_in_testing', 'In Testing'),
            ('hours_in_to_deploy', 'To Deploy')
        ]:
            avg_hours = post_dev_df[col].mean()
            percentage = (avg_hours / avg_total * 100) if avg_total > 0 else 0
            print(f"    • {label:20s}: {avg_hours / 24:4.1f} business days ({percentage:3.0f}%)")

        return post_dev_df

    def calculate_deployment_frequency(self):
        """Calculate how many stories shipped to Done per day"""
        done_transitions = self.df[self.df['to_status'] == 'Done'].copy()
        # Ensure transition_timestamp is datetime type
        done_transitions['transition_timestamp'] = pd.to_datetime(done_transitions['transition_timestamp'])
        done_transitions['date'] = done_transitions['transition_timestamp'].dt.date

        daily_deployments = done_transitions.groupby('date').size()

        print("\n" + "="*70)
        print("🚀 DEPLOYMENT FREQUENCY")
        print("="*70)

        print(f"\n📊 Sprint Deployment Stats:")
        print(f"  • Total stories shipped to Done: {len(done_transitions)}")
        print(f"  • Sprint duration: {len(daily_deployments)} days")
        print(f"  • Average deploys per day: {daily_deployments.mean():.1f}")
        print(f"  • Days with zero deploys: {(daily_deployments == 0).sum()}")
        print(f"  • Max deploys in a day: {daily_deployments.max()}")

        print(f"\n📅 Daily Deployment Activity:")
        for date, count in daily_deployments.items():
            bar = "█" * int(count)
            print(f"  {date}: {bar} ({count})")

        return daily_deployments

    def generate_visualizations(self, cycle_df, output_dir="sprint_analysis"):
        """Create visualization charts"""
        Path(output_dir).mkdir(exist_ok=True)

        # Set style
        sns.set_style("whitegrid")

        # 1. Time in To Deploy distribution
        if 'hours_in_to_deploy' in cycle_df.columns:
            deploy_data = cycle_df[cycle_df['hours_in_to_deploy'].notna()].copy()
            deploy_data['days_in_to_deploy'] = deploy_data['hours_in_to_deploy'] / 24

            fig, ax = plt.subplots(figsize=(10, 6))
            ax.hist(deploy_data['days_in_to_deploy'], bins=20, edgecolor='black', alpha=0.7)
            ax.axvline(deploy_data['days_in_to_deploy'].mean(), color='red',
                      linestyle='--', linewidth=2, label=f'Mean: {deploy_data["days_in_to_deploy"].mean():.1f}d')
            ax.axvline(deploy_data['days_in_to_deploy'].median(), color='green',
                      linestyle='--', linewidth=2, label=f'Median: {deploy_data["days_in_to_deploy"].median():.1f}d')
            ax.set_xlabel('Business Days in "To Deploy"', fontsize=12)
            ax.set_ylabel('Number of Issues', fontsize=12)
            ax.set_title('Distribution of Time Spent in "To Deploy" State\n(Weekends Excluded)', fontsize=14, fontweight='bold')
            ax.legend()
            plt.tight_layout()
            plt.savefig(f'{output_dir}/to_deploy_distribution.png', dpi=300)
            print(f"\n📈 Saved: {output_dir}/to_deploy_distribution.png")
            plt.close()

        # 2. Deployment frequency over time
        done_transitions = self.df[self.df['to_status'] == 'Done'].copy()
        # Ensure transition_timestamp is datetime type
        done_transitions['transition_timestamp'] = pd.to_datetime(done_transitions['transition_timestamp'])
        done_transitions['date'] = done_transitions['transition_timestamp'].dt.date
        daily_deployments = done_transitions.groupby('date').size()

        fig, ax = plt.subplots(figsize=(12, 6))
        ax.bar(range(len(daily_deployments)), daily_deployments.values, alpha=0.7)
        ax.axhline(daily_deployments.mean(), color='red', linestyle='--',
                  linewidth=2, label=f'Average: {daily_deployments.mean():.1f}/day')
        ax.set_xlabel('Sprint Days', fontsize=12)
        ax.set_ylabel('Stories Deployed', fontsize=12)
        ax.set_title('Daily Deployment Frequency', fontsize=14, fontweight='bold')
        ax.set_xticks(range(len(daily_deployments)))
        ax.set_xticklabels([d.strftime('%m/%d') for d in daily_deployments.index], rotation=45)
        ax.legend()
        plt.tight_layout()
        plt.savefig(f'{output_dir}/deployment_frequency.png', dpi=300)
        print(f"📈 Saved: {output_dir}/deployment_frequency.png")
        plt.close()

    def run_full_analysis(self,
                          generate_charts=True,
                          output_dir="sprint_analysis"):
        """Run complete analysis pipeline"""
        print("\n" + "="*70)
        print("🔍 SPRINT CYCLE TIME ANALYSIS")
        print("    Excluding Weekends from Calculations")
        print("="*70)

        # Calculate time in states
        cycle_df = self.calculate_time_in_states()

        # Run analyses
        self.analyze_deployment_bottleneck(cycle_df)
        self.analyze_post_dev_cycle_time(cycle_df)
        self.calculate_deployment_frequency()

        # Generate visualizations
        if generate_charts:
            self.generate_visualizations(cycle_df, output_dir=output_dir)

        print("\n" + "="*70)
        print("✅ ANALYSIS COMPLETE")
        print("="*70)

        return cycle_df


def main():
    """Example usage"""
    # Replace with your CSV file path
    parser = argparse.ArgumentParser(description="graph cycle time from CSV")
    parser.add_argument("-c", "--csv", type=str, help="path to CSV file")
    parser.add_argument("-d", "--dir", type=str, help="directory to save charts")
    args = parser.parse_args()
    csv_file = args.csv
    directory = Path(args.dir)

    analyzer = SprintCycleTimeAnalyzer(csv_file)
    cycle_data = analyzer.run_full_analysis(generate_charts=True,
                                            output_dir=directory)

    # Optionally save the processed data
    cycle_data.to_csv(directory / "cycle_time_analysis.csv", index=False)
    print("\n💾 Saved detailed cycle time data to: cycle_time_analysis.csv")


if __name__ == "__main__":
    main()
