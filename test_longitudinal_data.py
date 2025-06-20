#!/usr/bin/env python3
"""
Generate synthetic longitudinal data for testing the enwiden functionality.
Creates data with ursi + session_num structure for testing pivot operations.
"""

import os
import random

import numpy as np
import pandas as pd


def generate_longitudinal_test_data():
    """Generate synthetic longitudinal data for testing pivot functionality."""

    # Set random seed for reproducibility
    np.random.seed(42)
    random.seed(42)

    # Parameters
    n_subjects = 50
    sessions = ['visit1','visit2','visit3']

    # Generate base participant data
    subjects = [f"SUB{i:03d}" for i in range(1, n_subjects + 1)]

    # Create output directory
    output_dir = "test_longitudinal"
    os.makedirs(output_dir, exist_ok=True)

    # Demographics (static across sessions, but includes session for completeness)
    demo_data = []
    for subject in subjects:
        base_age = np.random.randint(18, 65)
        sex = np.random.choice([1.0, 2.0])  # 1=Female, 2=Male

        for session in sessions:
            demo_data.append({
                'ursi': subject,
                'session_num': session,
                'age': base_age,  # Static - same across sessions
                'sex': sex,       # Static - same across sessions
                'height': np.random.normal(170, 10),  # Static with small variation
                'weight': np.random.normal(70, 15)    # May change slightly over time
            })

    demo_df = pd.DataFrame(demo_data)
    demo_df.to_csv(os.path.join(output_dir, 'demographics.csv'), index=False)

    # Flanker task (changes across sessions)
    flanker_data = []
    for subject in subjects:
        for session in sessions:
            # Performance may improve over sessions
            session_factor = 1 + 0.05 * sessions.index(session)

            flanker_data.append({
                'ursi': subject,
                'session_num': session,
                'rt_congruent': np.random.normal(500, 50) / session_factor,
                'rt_incongruent': np.random.normal(550, 60) / session_factor,
                'accuracy': min(0.95, np.random.normal(0.85, 0.1) * session_factor),
                'interference_effect': np.random.normal(50, 20)
            })

    flanker_df = pd.DataFrame(flanker_data)
    flanker_df.to_csv(os.path.join(output_dir, 'flanker.csv'), index=False)

    # Cognitive assessment (may change over sessions)
    cognitive_data = []
    for subject in subjects:
        for session in sessions:
            # Some learning effects
            session_bonus = sessions.index(session) * 2

            cognitive_data.append({
                'ursi': subject,
                'session_num': session,
                'working_memory': np.random.normal(100, 15) + session_bonus,
                'processing_speed': np.random.normal(50, 10) + session_bonus,
                'attention_score': np.random.normal(75, 12) + session_bonus
            })

    cognitive_df = pd.DataFrame(cognitive_data)
    cognitive_df.to_csv(os.path.join(output_dir, 'cognitive.csv'), index=False)

    print(f"Generated longitudinal test data in '{output_dir}' directory:")
    print(f"- {len(subjects)} subjects across {len(sessions)} sessions")
    print(f"- Total rows per file: {len(subjects) * len(sessions)}")
    print(f"- Sessions: {', '.join(sessions)}")
    print("- Files: demographics.csv, flanker.csv, cognitive.csv")

if __name__ == "__main__":
    generate_longitudinal_test_data()
